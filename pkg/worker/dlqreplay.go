package worker

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// DLQ replay (protocol §2.4, server/dlq_replay.py): the single manual
// re-injection entry point. Guarantees:
//
//  1. one-shot marker per (dlq_key, dlq_id): SET NX EX 7d; a second replay
//     returns already_replayed with the original target_message_id;
//  2. a start carrier without execution_id gets a DETERMINISTIC uuid5 id, so
//     even a forced replay after marker expiry converges on one execution;
//  3. the target envelope inherits the SOURCE schema_version (never this
//     build's), and an over-version carrier is rejected before enqueue;
//  4. every attempt (including rejections) writes the audit stream; the DLQ
//     original is never deleted.
//
// The audit stream and marker keys live under the configured key prefix, so a
// shadow replay can never write production keys.

// Replay rejection reasons (stable strings, mirroring dlq_replay.py).
const (
	ReplayReasonReplayed    = "replayed"
	ReplayReasonAlready     = "already_replayed"
	ReplayReasonNotReplay   = "not_replayable"
	ReplayReasonSchemaNew   = "schema_too_new"
	ReplayReasonTenant      = "tenant_mismatch"
	ReplayReasonExecutionID = "execution_id_mismatch"
	ReplayReasonDeleted     = "execution_deleted"
	ReplayReasonEnqueueFail = "enqueue_failed"
	// #67 (A69-04): the payload has no timestamp and neither dead_lettered_at
	// nor the DLQ stream id is parseable — replaying would reset the carrier
	// age and wash it past the 30-day age gate.
	ReplayReasonCarrierTimestampUnknown = "carrier_timestamp_unknown"
)

// ReplayMarkerTTL is REPLAY_MARKER_TTL_SECONDS (7 days, same as start dedup).
const ReplayMarkerTTL = 7 * 86400 * time.Second

// replayUUIDNamespaceURL is uuid.NAMESPACE_URL.
var replayUUIDNamespaceURL = [16]byte{
	0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1,
	0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
}

// ReplayOptions configures one replay.
type ReplayOptions struct {
	DLQKey string
	DLQID  string
	// TargetStream defaults to the worker queue.
	TargetStream string
	Actor        string
	// Force bypasses an existing replay marker (still keeps the deterministic
	// execution id, so a forced replay cannot fan out into two executions).
	Force bool
	// ExecutionID / TenantID only fill carriers that lack the field; a
	// mismatch against a carrier-carried value is rejected.
	ExecutionID      string
	TenantID         string
	MaxSchemaVersion int
}

// ReplayResult is the dlq_replay.ReplayResult mirror.
type ReplayResult struct {
	Replayed          bool   `json:"replayed"`
	Reason            string `json:"reason"`
	TargetMessageID   string `json:"target_message_id,omitempty"`
	ExecutionID       string `json:"execution_id,omitempty"`
	TenantID          string `json:"tenant_id,omitempty"`
	SchemaVersion     int    `json:"schema_version,omitempty"`
	Detail            string `json:"detail,omitempty"`
	UsedDeterministic bool   `json:"minted_execution_id,omitempty"`
}

func (w *Worker) replayMarkerKey(dlqKey, dlqID string) string {
	return w.cfg.KeyPrefix + ":dlq:replay:" + dlqKey + ":" + dlqID
}

func (w *Worker) replayAuditKey() string {
	return w.cfg.KeyPrefix + ":dlq:replay:audit"
}

func (w *Worker) tombstoneKey(executionID string) string {
	// Platform-root key (§8.3), not tenant-routed.
	return w.cfg.KeyPrefix + ":exec_tombstone:" + executionID
}

// deterministicReplayExecutionID is uuid5(NAMESPACE_URL,
// "plaita:dlq:replay:{dlqKey}:{dlqID}") — the Python marker prefix is kept
// verbatim so an id minted by either runtime matches.
func deterministicReplayExecutionID(dlqKey, dlqID string) string {
	name := "plaita:dlq:replay:" + dlqKey + ":" + dlqID
	h := sha1.New()
	_, _ = h.Write(replayUUIDNamespaceURL[:])
	_, _ = h.Write([]byte(name))
	sum := h.Sum(nil)
	var b [16]byte
	copy(b[:], sum[:16])
	b[6] = (b[6] & 0x0f) | 0x50 // version 5
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return hex.EncodeToString(b[:])
}

// ReplayDeadLetter performs one idempotent replay.
func (w *Worker) ReplayDeadLetter(ctx context.Context, opts ReplayOptions) *ReplayResult {
	if opts.DLQKey == "" {
		opts.DLQKey = w.cfg.DLQKey
	}
	if opts.TargetStream == "" {
		opts.TargetStream = w.cfg.QueueName
	}
	if opts.Actor == "" {
		opts.Actor = w.cfg.DispatcherID
	}
	if opts.MaxSchemaVersion <= 0 {
		opts.MaxSchemaVersion = TaskSchemaVersion
	}
	if opts.DLQKey == "" || opts.DLQID == "" || opts.TargetStream == "" {
		return w.rejectReplay(ctx, opts, ReplayReasonNotReplay, "dlq_key/dlq_id/target_stream are required")
	}

	msgs, err := w.rdb.XRangeN(ctx, opts.DLQKey, opts.DLQID, opts.DLQID, 1).Result()
	if err != nil {
		return w.rejectReplay(ctx, opts, ReplayReasonNotReplay, "xrange failed: "+err.Error())
	}
	if len(msgs) == 0 {
		return w.rejectReplay(ctx, opts, ReplayReasonNotReplay, "dlq entry not found")
	}
	msgID := msgs[0].ID
	raw, _ := msgs[0].Values["payload"].(string)

	var envelope map[string]any
	if raw == "" || json.Unmarshal([]byte(raw), &envelope) != nil || envelope == nil {
		return w.rejectReplay(ctx, opts, ReplayReasonNotReplay, "dlq envelope is not a JSON object")
	}
	sourceID, _ := envelope["source_id"].(string)
	if truncated, _ := envelope["truncated"].(bool); truncated {
		return w.rejectReplay(ctx, opts, ReplayReasonNotReplay, "dead-letter payload was truncated; cannot replay safely")
	}
	body, ok := envelope["payload"].(map[string]any)
	if !ok || body == nil {
		return w.rejectReplay(ctx, opts, ReplayReasonNotReplay, "dead-letter payload is not a dict (raw/truncated)")
	}
	taskType, _ := body["type"].(string)
	if taskType != "start" && taskType != "resume" {
		return w.rejectReplay(ctx, opts, ReplayReasonNotReplay, fmt.Sprintf("unsupported task type: %q", taskType))
	}

	schemaVersion := 1 // LEGACY_TASK_SCHEMA_VERSION: missing never means "current"
	if rawSchema, present := envelope["schema_version"]; present && rawSchema != nil {
		parsed, perr := schemaVersionOf(rawSchema)
		if perr != nil {
			return w.rejectReplay(ctx, opts, ReplayReasonNotReplay, "invalid schema_version: "+fmt.Sprint(rawSchema))
		}
		schemaVersion = parsed
	}
	if schemaVersion > opts.MaxSchemaVersion {
		res := &ReplayResult{Reason: ReplayReasonSchemaNew, SchemaVersion: schemaVersion,
			Detail: fmt.Sprintf("schema_version=%d > max=%d", schemaVersion, opts.MaxSchemaVersion)}
		w.auditReplay(ctx, opts, res, msgID, sourceID)
		return res
	}

	// Tenant: the carrier's own value wins; an explicit value may only fill a
	// missing field. A cross-tenant explicit value is rejected.
	owner, _ := body["tenant_id"].(string)
	if owner == "" {
		if opts.TenantID == "" {
			return w.rejectReplay(ctx, opts, ReplayReasonNotReplay,
				"payload has no explicit tenant_id; pass tenant_id explicitly")
		}
		owner = opts.TenantID
	}
	if opts.TenantID != "" && opts.TenantID != owner {
		res := &ReplayResult{Reason: ReplayReasonTenant, TenantID: owner,
			Detail: fmt.Sprintf("payload tenant %q != explicit tenant %q", owner, opts.TenantID)}
		w.auditReplay(ctx, opts, res, msgID, sourceID)
		return res
	}

	carrierExecutionID, _ := body["execution_id"].(string)
	minted := false
	if carrierExecutionID == "" {
		switch {
		case opts.ExecutionID != "":
			carrierExecutionID = opts.ExecutionID
		case taskType == "start":
			carrierExecutionID = deterministicReplayExecutionID(opts.DLQKey, msgID)
			minted = true
		default:
			return w.rejectReplay(ctx, opts, ReplayReasonNotReplay, "resume carrier has no execution_id")
		}
	} else if opts.ExecutionID != "" && opts.ExecutionID != carrierExecutionID {
		res := &ReplayResult{Reason: ReplayReasonExecutionID, ExecutionID: carrierExecutionID,
			Detail: fmt.Sprintf("payload execution_id %q != explicit %q", carrierExecutionID, opts.ExecutionID)}
		w.auditReplay(ctx, opts, res, msgID, sourceID)
		return res
	}

	// A deleted execution must not be resurrected by a replay.
	if _, err := w.rdb.Get(ctx, w.tombstoneKey(carrierExecutionID)).Result(); err == nil {
		res := &ReplayResult{Reason: ReplayReasonDeleted, ExecutionID: carrierExecutionID, TenantID: owner,
			Detail: "execution tombstone exists; replay would resurrect a deleted run"}
		w.auditReplay(ctx, opts, res, msgID, sourceID)
		return res
	} else if !errors.Is(err, redis.Nil) {
		return w.rejectReplay(ctx, opts, ReplayReasonNotReplay, "tombstone check failed: "+err.Error())
	}

	// A69-04 (#67): replay must not wash an old carrier into a fresh one. A
	// replayed entry is XADDed with a NOW stream id, and the age gate takes
	// the MAX of payload timestamp and stream id — so a payload without a
	// timestamp would measure ~0s old and sail past the 30-day horizon.
	// Resolve a real carrier timestamp first; both sources unparseable ⇒
	// named fail-closed rejection (before the marker, so the entry stays
	// replayable after the data is fixed). now() is never a fallback.
	if ts, ok := body["timestamp"].(string); !ok || strings.TrimSpace(ts) == "" {
		carrierTs, resolved := deadLetterCarrierTimestamp(envelope, msgID)
		if !resolved {
			res := &ReplayResult{Reason: ReplayReasonCarrierTimestampUnknown, ExecutionID: carrierExecutionID,
				TenantID: owner, Detail: "dead-letter payload has no timestamp and neither " +
					"dead_lettered_at nor dlq stream id is parseable; replay would reset the carrier age"}
			w.auditReplay(ctx, opts, res, msgID, sourceID)
			return res
		}
		body["timestamp"] = carrierTs
	}

	markerKey := w.replayMarkerKey(opts.DLQKey, msgID)
	marker := map[string]any{
		"actor":             opts.Actor,
		"ts":                floatEpoch(),
		"dlq_key":           opts.DLQKey,
		"dlq_id":            msgID,
		"tenant_id":         owner,
		"schema_version":    schemaVersion,
		"execution_id":      carrierExecutionID,
		"target_stream":     opts.TargetStream,
		"target_message_id": nil,
		"attempts":          1,
		"force":             opts.Force,
	}
	blob, _ := json.Marshal(marker)
	if opts.Force {
		if err := w.rdb.Set(ctx, markerKey, string(blob), ReplayMarkerTTL).Err(); err != nil {
			return w.rejectReplay(ctx, opts, ReplayReasonNotReplay, "replay marker write failed: "+err.Error())
		}
	} else {
		acquired, err := w.rdb.SetNX(ctx, markerKey, string(blob), ReplayMarkerTTL).Result()
		if err != nil {
			return w.rejectReplay(ctx, opts, ReplayReasonNotReplay, "replay marker write failed: "+err.Error())
		}
		if !acquired {
			res := &ReplayResult{Reason: ReplayReasonAlready}
			if existing, rerr := w.rdb.Get(ctx, markerKey).Result(); rerr == nil {
				var prior map[string]any
				if json.Unmarshal([]byte(existing), &prior) == nil {
					res.TargetMessageID, _ = prior["target_message_id"].(string)
					res.ExecutionID, _ = prior["execution_id"].(string)
					res.TenantID, _ = prior["tenant_id"].(string)
					if n, ok := prior["schema_version"].(float64); ok {
						res.SchemaVersion = int(n)
					}
				}
			}
			res.Detail = "replay marker exists; use force to re-enqueue deliberately"
			w.auditReplay(ctx, opts, res, msgID, sourceID)
			return res
		}
	}

	body["execution_id"] = carrierExecutionID
	if _, present := body["tenant_id"]; !present || body["tenant_id"] == "" {
		body["tenant_id"] = owner
	}
	if minted {
		body["dedup_key"] = "dlq-replay:" + carrierExecutionID
	}
	payload, merr := json.Marshal(body)
	if merr != nil {
		_ = w.rdb.Del(ctx, markerKey).Err()
		return w.rejectReplay(ctx, opts, ReplayReasonNotReplay, "payload marshal failed: "+merr.Error())
	}
	targetID, err := w.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: opts.TargetStream,
		Values: map[string]any{
			"payload":        string(payload),
			"schema_version": strconv.Itoa(schemaVersion),
		},
	}).Result()
	if err != nil {
		// Undo the marker so a retry can proceed; the undo itself is
		// best-effort but always logged.
		if derr := w.rdb.Del(ctx, markerKey).Err(); derr != nil {
			w.logger.Warn("dlq replay marker undo failed", "key", markerKey, "error", derr)
		}
		res := &ReplayResult{Reason: ReplayReasonEnqueueFail, ExecutionID: carrierExecutionID,
			TenantID: owner, SchemaVersion: schemaVersion, Detail: err.Error()}
		w.auditReplay(ctx, opts, res, msgID, sourceID)
		return res
	}

	marker["target_message_id"] = targetID
	if updated, merr := json.Marshal(marker); merr == nil {
		if err := w.rdb.Set(ctx, markerKey, string(updated), ReplayMarkerTTL).Err(); err != nil {
			w.logger.Warn("dlq replay marker update failed (marker stays without target)",
				"key", markerKey, "error", err)
		}
	}
	res := &ReplayResult{
		Replayed: true, Reason: ReplayReasonReplayed, TargetMessageID: targetID,
		ExecutionID: carrierExecutionID, TenantID: owner, SchemaVersion: schemaVersion,
		UsedDeterministic: minted,
	}
	w.auditReplay(ctx, opts, res, msgID, sourceID)
	return res
}

func (w *Worker) rejectReplay(ctx context.Context, opts ReplayOptions, reason, detail string) *ReplayResult {
	res := &ReplayResult{Reason: reason, Detail: detail}
	w.auditReplay(ctx, opts, res, opts.DLQID, "")
	return res
}

// auditReplay appends one audit row (best-effort; a failure never rolls back
// an already-enqueued replay).
func (w *Worker) auditReplay(ctx context.Context, opts ReplayOptions, res *ReplayResult, dlqID, sourceID string) {
	_, err := w.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: w.replayAuditKey(),
		Values: map[string]any{
			"ts":                strconv.FormatFloat(floatEpoch(), 'f', 6, 64),
			"action":            "dlq.replay",
			"actor":             opts.Actor,
			"dlq_key":           opts.DLQKey,
			"dlq_id":            dlqID,
			"source_id":         sourceID,
			"target_stream":     opts.TargetStream,
			"target_message_id": res.TargetMessageID,
			"execution_id":      res.ExecutionID,
			"tenant_id":         res.TenantID,
			"schema_version":    strconv.Itoa(res.SchemaVersion),
			"result":            res.Reason,
			"force":             boolString(opts.Force),
		},
	}).Result()
	if err != nil {
		w.logger.Warn("dlq replay audit write failed", "dlq_id", dlqID, "error", err)
	}
}

func boolString(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// schemaVersionOf accepts the string/number shapes a Redis stream field can
// surface.
func schemaVersionOf(v any) (int, error) {
	switch n := v.(type) {
	case string:
		if n == "" {
			return 1, nil
		}
		return strconv.Atoi(n)
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64:
		return int(n), nil
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

// deadLetterCarrierTimestamp resolves the A69-04 carrier timestamp (#67):
// the DLQ envelope's dead_lettered_at first (JSON number = float epoch, as
// both runtimes write it; else numeric string; else ISO string), then the DLQ
// entry's stream id millisecond prefix (same parse as the age gate's
// taskAgeFromStreamID). The result is a naive local ISO string that
// parseProducerTimestamp accepts. Both sources unresolvable → not-ok; the
// caller must reject fail-closed (now() is never a fallback — Python
// dlq_replay._dead_letter_carrier_timestamp 口径).
func deadLetterCarrierTimestamp(envelope map[string]any, dlqMessageID string) (string, bool) {
	switch raw := envelope["dead_lettered_at"].(type) {
	case float64:
		if t, ok := epochSecondsToTime(raw); ok {
			return naiveLocalISO(t), true
		}
	case string:
		if s := strings.TrimSpace(raw); s != "" {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				if t, ok := epochSecondsToTime(f); ok {
					return naiveLocalISO(t), true
				}
			} else if t, ok := parseProducerTimestamp(s); ok {
				return naiveLocalISO(t), true
			}
		}
	}
	if t, ok := streamIDTime(dlqMessageID); ok {
		return naiveLocalISO(t), true
	}
	return "", false
}

// epochSecondsToTime converts a float epoch (seconds) and rejects values
// outside the representable wall-clock range (Python fromtimestamp's
// OverflowError/OSError 口径 — an absurd epoch must not fabricate an age).
func epochSecondsToTime(f float64) (time.Time, bool) {
	if math.IsNaN(f) || math.IsInf(f, 0) || f < -62135596800 || f > 253402300799 {
		return time.Time{}, false
	}
	sec := int64(math.Floor(f))
	return time.Unix(sec, int64(math.Round((f-float64(sec))*1e9))), true
}

// naiveLocalISO formats a time the way Python datetime.isoformat() does for
// naive local datetimes (the producer timestamp shape, §2.3).
func naiveLocalISO(t time.Time) string {
	return t.Format("2006-01-02T15:04:05.999999")
}
