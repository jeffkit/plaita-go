package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/worker"
)

// Execution read handlers: the three read-only routes of the console BFF.
// Response shapes are field-compatible with the Python console
// (plaita-console/backend/api/executions.py); deviations carry a
// "contract note" comment at the emitting site.

// executionView mirrors the Python ExecutionInfo model field-for-field
// (snake_case JSON). Pointer/map fields render null when unset, matching
// the pydantic response that always contains every declared key.
//
// Contract notes vs Python:
//   - flow_name is carried in the Python summary projection but dropped by
//     the pydantic model (extra=ignore); this view keeps it — additive.
//   - timeline is Go-only in the detail body: Python exposes the same dict
//     through GET /executions/{id}/timeline instead. Additive; the envelope
//     itself is byte-shape identical (see eventsEnvelope).
//   - nodes/output (local-mode-only fields), node_usage and
//     langfuse_trace_url are always null in this subset.
type executionView struct {
	ExecutionID      string         `json:"execution_id"`
	FlowID           string         `json:"flow_id"`
	FlowVersion      *string        `json:"flow_version"`
	FlowName         *string        `json:"flow_name"`
	Status           string         `json:"status"`
	TenantID         string         `json:"tenant_id"`
	StartTime        *string        `json:"start_time"`
	EndTime          *string        `json:"end_time"`
	LastUpdateTime   *string        `json:"last_update_time"`
	Context          map[string]any `json:"context"`
	Error            map[string]any `json:"error"`
	Invoker          *string        `json:"invoker"`
	Nodes            any            `json:"nodes"`
	Output           any            `json:"output"`
	NodeTimings      map[string]any `json:"node_timings"`
	Usage            map[string]any `json:"usage"`
	NodeUsage        any            `json:"node_usage"`
	LangfuseTraceURL any            `json:"langfuse_trace_url"`
}

// executionDetailView is the detail body: the shared ExecutionInfo shape
// plus the embedded execution timeline (Go-only, see the contract notes
// above). List rows stay timeline-free so the list projection is byte-shape
// identical to the Python ExecutionInfo row set.
type executionDetailView struct {
	executionView
	Timeline map[string]any `json:"timeline"`
}

// executionListResponse mirrors the Python ExecutionListResponse.
type executionListResponse struct {
	Executions []executionView `json:"executions"`
	Total      int             `json:"total"`
	Page       int             `json:"page"`
	Size       int             `json:"size"`
	Truncated  bool            `json:"truncated"`
}

// eventsEnvelope mirrors the Python `_timeline_envelope` (the
// GET /executions/{id}/timeline response) and extends it with the
// incremental cursor fields required by issue #33 (分页/增量).
type eventsEnvelope struct {
	ExecutionID any              `json:"execution_id"`
	Status      any              `json:"status"`
	Source      string           `json:"source"`
	Collected   bool             `json:"collected"`
	Events      []map[string]any `json:"events"`
	Dropped     int              `json:"dropped"`
	MaxEvents   any              `json:"max_events"`
	EnqueuedAt  any              `json:"enqueued_at"`
	QueueWaitMS any              `json:"queue_wait_ms"`
	StartTime   any              `json:"start_time"`
	EndTime     any              `json:"end_time"`
	// Go extension (#33): incremental paging over the retained timeline.
	// after_seq echoes the request cursor; next_after_seq is the cursor a
	// follow-up request should pass; has_more reports whether the retained
	// window holds more events beyond this page.
	AfterSeq     int64 `json:"after_seq"`
	NextAfterSeq int64 `json:"next_after_seq"`
	HasMore      bool  `json:"has_more"`
}

// handleListExecutions serves GET /executions?page=&size=&status=&flow_id=.
// (`flow` is accepted as an issue-#33 alias of flow_id.)
//
// Contract note vs Python: the console reads ONE namespace per tenant scope
// and (A2R20-03) pages the index ZSET for unfiltered queries. This handler
// keeps the same response shape and semantics — total = index cardinality
// when unfiltered, matched-count lower bound + truncated=true when a filter
// runs into the candidate cap — but the filtered scan is a bounded pipeline
// projection of index members (candidate cap 10000), not the console's
// chunked Lua projection.
func (s *Server) handleListExecutions(w http.ResponseWriter, r *http.Request) {
	page, ok := queryInt(r, "page", 1, 1, maxPage)
	if !ok {
		writeError(w, http.StatusUnprocessableEntity, "page must be an integer between 1 and 10000")
		return
	}
	size, ok := queryInt(r, "size", defaultListSize, 1, s.listMaxSize)
	if !ok {
		writeError(w, http.StatusUnprocessableEntity,
			"size must be an integer between 1 and "+strconv.Itoa(s.listMaxSize))
		return
	}
	statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
	flowFilter := firstNonEmpty(
		strings.TrimSpace(r.URL.Query().Get("flow_id")),
		strings.TrimSpace(r.URL.Query().Get("flow")))
	tenant := identityTenant(r)
	ns := s.cfg.Namespace(tenant)

	members, total, truncated, err := s.listCandidates(r.Context(), ns, page, size, statusFilter != "" || flowFilter != "")
	if err != nil {
		s.writeInternal(w, r, "execution index read", err)
		return
	}
	rows := s.projectRows(r.Context(), members, tenant, statusFilter, flowFilter)
	sort.SliceStable(rows, func(i, j int) bool {
		return strOr(rows[i].StartTime) > strOr(rows[j].StartTime)
	})
	// Filtered totals are matched-count lower bounds once the candidate cap
	// truncated the scan (Python A2R20-03 wording); unfiltered totals are
	// exact index cardinalities.
	if statusFilter != "" || flowFilter != "" {
		total = len(rows)
	}
	offset := (page - 1) * size
	end := offset + size
	if offset > len(rows) {
		offset = len(rows)
	}
	if end > len(rows) {
		end = len(rows)
	}
	writeJSON(w, http.StatusOK, executionListResponse{
		Executions: rows[offset:end],
		Total:      total,
		Page:       page,
		Size:       size,
		Truncated:  truncated,
	})
}

// listCandidates resolves the candidate state keys for one list request.
// Unfiltered queries take a true index page (only offset+size members are
// projected, total = ZCARD — the console's A2R20-03 fast path); filtered
// queries scan at most filterMaxCandidates newest members.
func (s *Server) listCandidates(ctx context.Context, ns string, page, size int, filtered bool) ([]string, int, bool, error) {
	indexKey := ns + ":execution:index"
	card, err := s.rdb.ZCard(ctx, indexKey).Result()
	if err != nil {
		return nil, 0, false, err
	}
	if !filtered {
		need := (page-1)*size + size
		stop := int64(need) - 1
		if card < int64(need) {
			stop = card - 1
		}
		members, err := s.rdb.ZRevRange(ctx, indexKey, 0, stop).Result()
		if err != nil {
			return nil, 0, false, err
		}
		return members, int(card), false, nil
	}
	cap := int64(s.filterCap)
	truncated := card > cap
	stop := card - 1
	if truncated {
		stop = cap - 1
	}
	members, err := s.rdb.ZRevRange(ctx, indexKey, 0, stop).Result()
	if err != nil {
		return nil, 0, false, err
	}
	return members, 0, truncated, nil
}

// projectRows pipelines GET + decode of the candidate state keys and applies
// the status/flow filters. Read errors on individual rows skip the row (a
// corrupt row must not take the whole list down — same posture as the
// console's per-key fallback); a Redis pipeline failure fails the request.
func (s *Server) projectRows(ctx context.Context, members []string, tenant, statusFilter, flowFilter string) []executionView {
	rows := make([]executionView, 0, len(members))
	const chunk = 500
	for start := 0; start < len(members); start += chunk {
		end := start + chunk
		if end > len(members) {
			end = len(members)
		}
		batch := members[start:end]
		pipe := s.rdb.Pipeline()
		cmds := make([]*redis.StringCmd, len(batch))
		for i, key := range batch {
			cmds[i] = pipe.Get(ctx, key)
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			// Individual missing keys surface as redis.Nil inside the cmds;
			// a pipeline-level error is a real failure.
			s.logger.Warn("execution list projection failed", "error", err)
			continue
		}
		for i, cmd := range cmds {
			raw, err := cmd.Result()
			if err != nil {
				continue // vanished between index read and GET — skip the row
			}
			var state worker.ExecutionState
			if err := json.Unmarshal([]byte(raw), &state); err != nil {
				s.logger.Warn("execution list: corrupt state row skipped", "key", batch[i], "error", err)
				continue
			}
			if statusFilter != "" && state.Status != statusFilter {
				continue
			}
			if flowFilter != "" && state.FlowID != flowFilter {
				continue
			}
			rows = append(rows, summaryView(&state, tenant, batch[i]))
		}
	}
	return rows
}

// summaryView builds the list row: Python's list path deliberately leaves
// context/error/node data null (the full context must not enter console
// memory); this subset keeps that posture.
//
// tenant_id is derived from the request scope (i.e. the namespace actually
// read), the same ground truth as the console's _tenant_from_key — the
// state row's own tenant_id field is never trusted for display.
func summaryView(state *worker.ExecutionState, tenant, key string) executionView {
	return executionView{
		ExecutionID:    firstNonEmpty(state.ExecutionID, executionIDFromKey(key)),
		FlowID:         state.FlowID,
		FlowVersion:    strPtr(state.FlowVersion),
		FlowName:       strPtr(state.FlowName),
		Status:         firstNonEmpty(state.Status, "unknown"),
		TenantID:       tenantScopeDefault(tenant),
		StartTime:      strPtr(state.StartTime),
		EndTime:        strPtr(state.EndTime),
		LastUpdateTime: strPtr(state.LastUpdateTime),
		Invoker:        strPtr(state.Invoker),
		Usage:          nonEmptyMap(state.Usage),
	}
}

// handleGetExecution serves GET /executions/{id}: full state (status / flow /
// tenant / timeline / node timings / usage).
func (s *Server) handleGetExecution(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenant := identityTenant(r)
	state, err := s.store.LoadExecutionState(r.Context(), tenant, id)
	if err != nil {
		s.writeInternal(w, r, "execution state load", err)
		return
	}
	if state == nil {
		notFound(w, id)
		return
	}
	view := summaryView(state, tenant, "")
	view.Context = state.Context
	view.Error = state.Error
	view.NodeTimings = state.NodeTimings
	writeJSON(w, http.StatusOK, executionDetailView{executionView: view, Timeline: state.Timeline})
}

// handleExecutionEvents serves GET /executions/{id}/events — the event
// stream read side (§6.2 persisted half; the live half is the
// `{prefix}:execution:events:{id}` Pub/Sub channel, which stays
// console-SSE territory).
//
// Paging/incremental contract (#33): `after_seq` (default 0) returns only
// events with seq > after_seq — the Go worker keeps seq monotonic across
// resume (worker.seedRunFromState), so the cursor survives suspend/resume;
// `limit` (default 100, cap 500) bounds the page; has_more/next_after_seq
// drive the follow-up.
func (s *Server) handleExecutionEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	afterSeq, ok := queryInt64(r, "after_seq", 0, 0)
	if !ok {
		writeError(w, http.StatusUnprocessableEntity, "after_seq must be a non-negative integer")
		return
	}
	limit, ok := queryInt(r, "limit", defaultEventsLimit, 1, s.eventsMaxLimit)
	if !ok {
		writeError(w, http.StatusUnprocessableEntity,
			"limit must be an integer between 1 and "+strconv.Itoa(s.eventsMaxLimit))
		return
	}
	tenant := identityTenant(r)
	state, err := s.store.LoadExecutionState(r.Context(), tenant, id)
	if err != nil {
		s.writeInternal(w, r, "execution state load", err)
		return
	}
	if state == nil {
		notFound(w, id)
		return
	}
	writeJSON(w, http.StatusOK, eventsView(state, afterSeq, limit))
}

// eventsView projects the state timeline into the Python envelope shape
// (`_timeline_envelope`) plus the #33 cursor fields.
//
// collected=false = the execution has no timeline key at all (pre-#25-era
// state or an engine that never collected) — "not collected", NOT "empty
// log"; the console's readers are forbidden from folding null queue_wait_ms
// into 0 and this projection keeps that distinction.
func eventsView(state *worker.ExecutionState, afterSeq int64, limit int) eventsEnvelope {
	tl := state.Timeline
	rawEvents, _ := tl["events"].([]any)
	env := eventsEnvelope{
		ExecutionID: state.ExecutionID,
		Status:      state.Status,
		Source:      "state",
		Collected:   tl != nil,
		Events:      []map[string]any{},
		Dropped:     intOf(tl["dropped"]),
		MaxEvents:   tl["max_events"],
		EnqueuedAt:  tl["enqueued_at"],
		QueueWaitMS: tl["queue_wait_ms"],
		StartTime:   nonEmptyString(state.StartTime),
		EndTime:     nonEmptyString(state.EndTime),
		AfterSeq:    afterSeq,
	}
	env.NextAfterSeq = afterSeq
	for _, raw := range rawEvents {
		event, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		seq := int64Of(event["seq"])
		if seq <= afterSeq {
			continue
		}
		if len(env.Events) >= limit {
			env.HasMore = true
			break
		}
		env.Events = append(env.Events, event)
		if seq > env.NextAfterSeq {
			env.NextAfterSeq = seq
		}
	}
	return env
}

// ---- small helpers ----

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func strOr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func nonEmptyMap(m map[string]any) map[string]any {
	if len(m) == 0 {
		return nil
	}
	return m
}

func nonEmptyString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func intOf(v any) int {
	n, _ := numberOf(v)
	return int(n)
}

func int64Of(v any) int64 {
	n, _ := numberOf(v)
	return int64(n)
}

// numberOf accepts the JSON-roundtrip float64 as well as Go-native ints.
func numberOf(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	}
	return 0, false
}

// tenantScopeDefault renders the response tenant_id for a request scoped to
// tenant: "" and the configured default both answer "default" (Python
// _tenant_from_key). Kept as a closure-free helper so summaryView does not
// need the whole config.
func tenantScopeDefault(requested string) string {
	if requested == "" {
		return "default"
	}
	return requested
}

// executionIDFromKey recovers the execution id from an index member
// (`{ns}:execution:{id}`), the same parse as the console's
// _execution_id_from_key. Used only when a state row lacks execution_id.
func executionIDFromKey(key string) string {
	if _, rest, found := strings.Cut(key, ":execution:"); found {
		return rest
	}
	return key
}
