package worker

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// Task age gate (issue #29, Python B52-02/A69-04): the main task stream has
// no time TTL, so a carrier can survive arbitrarily long in backlog/PEL.
// Past the configured horizon (PLAITA_TASK_MAX_AGE_SECONDS, default 30 days;
// negative disables) such carriers are acked WITHOUT executing and the drop
// is made observable (Warn log + task_expired_total + a live event on
// {prefix}:execution:events:{id}).
//
// Age is the MAX of two sources (Python _start_carrier_too_old):
//
//   - the payload `timestamp` (producer enqueue wall clock) — a re-enqueued
//     copy inherits it, so requeues cannot wash the original age;
//   - the Stream ID millisecond prefix — a fresh re-enqueue gets a fresh ID,
//     so a future/forged timestamp cannot whitewash an old Stream ID either.
//
// When NEITHER source is determinable the gate fails closed (drop + count):
// admitting an age-unknown carrier would defeat the horizon entirely. In
// practice this only happens for manually XADDed entries with non-numeric
// ids; the payload timestamp alone also covers legacy producers that never
// carried one (the gate still applies, measuring from the write time).

// taskAgeFromTimestamp parses the producer timestamp (naive local wall clock
// or RFC3339, §2.3) into an age in seconds. Unparsable → not reported.
func taskAgeFromTimestamp(ts string) (float64, bool) {
	if ts == "" {
		return 0, false
	}
	t, ok := parseProducerTimestamp(ts)
	if !ok {
		return 0, false
	}
	return time.Since(t).Seconds(), true
}

// streamIDTime parses a Redis stream id ("<ms>-<seq>") into its write time.
// Only purely numeric ids with a positive millisecond prefix carry a usable
// write time (Python timestamp_from_stream_id 口径); "0-1" and non-numeric
// ids report not-ok.
func streamIDTime(messageID string) (time.Time, bool) {
	dash := strings.IndexByte(messageID, '-')
	if dash <= 0 {
		return time.Time{}, false
	}
	ms, err := strconv.ParseInt(messageID[:dash], 10, 64)
	if err != nil || ms <= 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}

// taskAgeFromStreamID parses a Redis stream id ("<ms>-<seq>") into an age in
// seconds. Only purely numeric ids carry a usable write time (Python
// timestamp_from_stream_id 口径).
func taskAgeFromStreamID(messageID string) (float64, bool) {
	t, ok := streamIDTime(messageID)
	if !ok {
		return 0, false
	}
	return time.Since(t).Seconds(), true
}

// carrierAgeSeconds is the dual-source max age (payload timestamp vs Stream
// ID). Every parsable source participates in the max — including negative
// ages (Python max(ages) 口径): a negative max means the worker clock is
// behind the producer clock, i.e. the carrier is fresher than "now" — that is
// freshness, clamped to 0 ("just enqueued"), NOT an unknown age (#59: the old
// `best := -1.0` sentinel merged "both unparsable" and "both future-dated"
// into one known=false verdict and fail-closed dropped fresh carriers).
// known=false only when NEITHER source is parsable.
func carrierAgeSeconds(task *Task, messageID string) (age float64, known bool) {
	best := math.Inf(-1)
	if a, ok := taskAgeFromTimestamp(task.Timestamp); ok {
		known = true
		if a > best {
			best = a
		}
	}
	if a, ok := taskAgeFromStreamID(messageID); ok {
		known = true
		if a > best {
			best = a
		}
	}
	if !known {
		return 0, false
	}
	if best < 0 {
		best = 0
	}
	return best, true
}

// taskAgeOf is the dual-source max age for logs/events (0 when
// undeterminable — the fail-closed drop is counted separately).
func taskAgeOf(task *Task, messageID string) float64 {
	age, _ := carrierAgeSeconds(task, messageID)
	return age
}

// carrierTooOld reports whether a start/resume carrier is past the age
// horizon and must be dropped (acked without execution). maxAge < 0 disables
// the gate entirely (normalize maps 0/unset to the 30-day default).
func (w *Worker) carrierTooOld(task *Task, messageID string) bool {
	maxAge := float64(w.cfg.TaskMaxAgeSeconds)
	if maxAge < 0 {
		return false
	}
	age, known := carrierAgeSeconds(task, messageID)
	if !known {
		// Fail closed: an undeterminable carrier could have been backing up
		// since before the horizon; admitting it would void the gate. Name
		// each source's reason (#59): the old canned message asserted "no
		// timestamp, non-numeric stream id" even when both sources were
		// present (the future-dated shape, since fixed) — it sent debugging
		// toward the producer format.
		w.logger.Warn("task age undeterminable ("+carrierAgeUnknownReason(task, messageID)+"); dropping fail-closed",
			"type", task.Type, "id", messageID, "timestamp", task.Timestamp)
		w.metrics.IncCounter(MetricTaskAgeUnknownTotal, metricLabels("tenant", task.TenantID, "type", task.Type))
		return true
	}
	return age > maxAge
}

// carrierAgeUnknownReason names why each age source is unusable, so an
// operator can tell a legacy producer (no timestamp) from a forged/manual
// XADD (non-numeric stream id).
func carrierAgeUnknownReason(task *Task, messageID string) string {
	var parts []string
	if task.Timestamp == "" {
		parts = append(parts, "no timestamp")
	} else if _, ok := parseProducerTimestamp(task.Timestamp); !ok {
		parts = append(parts, "unparsable timestamp")
	}
	if _, ok := streamIDTime(messageID); !ok {
		parts = append(parts, "non-numeric stream id")
	}
	return strings.Join(parts, ", ")
}
