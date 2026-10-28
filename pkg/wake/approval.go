package wake

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/worker"
)

// ApprovalSource is the Go counterpart of Python plaita ApprovalService:
// it consumes the approval queue the worker's §5.2 dispatch writes
// (`{prefix}:approval:queue`) and creates the shared approval records that a
// decision surface (Python console, or any host calling SubmitApprovalDecision)
// resolves. Record keys, JSON shape, decision-lock protocol, strategy
// evaluation and the completion event shape are byte-compatible with the
// Python service, so mixed fleets share one approval state space:
//
//	{prefix}:approval:pending:{approval_id}  = record JSON (TTL 7d)
//	{prefix}:approval:lock:{approval_id}     = decision lock (SET NX, TTL 10s)
//	{prefix}:approval:queue                  = dispatch list (consumed here)
//
// Completion publishes the resume event to `{ns}:events:approval_decision`
// (the approval node's fixed event contract) and deletes the record —
// terminal state lives on in the execution row, not in the record.
type ApprovalSource struct {
	*listSource

	recordPrefix string
	lockPrefix   string
	recordTTL    time.Duration
	lockTTL      time.Duration
	lockWait     time.Duration
	lockPoll     time.Duration
	cfg          *Config
}

// releaseIfOwnedLua is the compare-and-del lock release (execution_lease
// style): only the holder deletes the lock, so a timed-out critical section
// handed to another holder is not torn down by the original owner's finally.
const releaseIfOwnedLua = `
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end`

// DecisionResult mirrors Python submit_approval_decision's return shape.
type DecisionResult struct {
	Status        string `json:"status"` // success | error
	FinalDecision string `json:"final_decision,omitempty"`
	Message       string `json:"message"`
}

func NewApprovalSource(cfg *Config, rdb *redis.Client) (*ApprovalSource, error) {
	core := newListSource(SourceApproval, cfg, rdb)
	s := &ApprovalSource{
		listSource:   core,
		cfg:          cfg,
		recordPrefix: cfg.KeyPrefix + ":approval:pending:",
		lockPrefix:   cfg.KeyPrefix + ":approval:lock:",
		recordTTL:    7 * 24 * time.Hour,
		lockTTL:      10 * time.Second,
		lockWait:     2 * time.Second,
		lockPoll:     50 * time.Millisecond,
	}
	pub := worker.NewEventPublisher(rdb, &worker.Config{
		KeyPrefix:       cfg.KeyPrefix,
		DefaultTenantID: cfg.DefaultTenantID,
		EventTTLSeconds: cfg.EventTTLSeconds,
	})
	s.publishFn = pub.Publish
	core.validate = s.validateTask
	core.preCheck = s.recordExists
	core.handle = s.handleIntake
	return s, nil
}

// validateTask mirrors Python ApprovalService.validate_task_config: base
// fields (node_id/event_type) + approval_config + approver_config.approvers.
func (s *ApprovalSource) validateTask(task map[string]any) bool {
	if str(task["node_id"]) == "" || str(task["event_type"]) == "" {
		return false
	}
	ac, _ := task["approval_config"].(map[string]any)
	pc, _ := task["approver_config"].(map[string]any)
	if ac == nil || pc == nil {
		return false
	}
	approvers, _ := pc["approvers"].([]any)
	return len(approvers) > 0
}

// recordExists is the duplicate-dispatch guard: an approval whose record is
// already on file is acked without rebuilding (a rebuild would wipe decisions
// already collected).
func (s *ApprovalSource) recordExists(ctx context.Context, task map[string]any) bool {
	id := str(task["approval_id"])
	if id == "" {
		return true
	}
	exists, err := s.rdb.Exists(ctx, s.recordPrefix+id).Result()
	if err != nil {
		s.logger.Warn("approval record lookup failed (treating as absent)", "approval_id", id, "error", err)
		return true
	}
	return exists == 0
}

// handleIntake creates the shared approval record (Python handle_task).
func (s *ApprovalSource) handleIntake(ctx context.Context, raw string, task map[string]any) bool {
	id := str(task["approval_id"])
	if id == "" {
		s.logger.Error("approval task without approval_id (dropped)", "raw", truncate(raw, 200))
		return true
	}
	approverCfg, _ := task["approver_config"].(map[string]any)
	strategy := "any"
	if v := str(approverCfg["strategy"]); v != "" {
		strategy = v
	}
	approvers, _ := approverCfg["approvers"].([]any)
	record := map[string]any{
		"approval_id":       id,
		"task_config":       task,
		"created_time":      time.Now().UnixMilli(),
		"status":            "pending",
		"approvals":         []any{},
		"required_approvers": approvers,
		"strategy":          strategy,
	}
	blob, err := json.Marshal(record)
	if err != nil {
		s.logger.Error("approval record marshal failed", "approval_id", id, "error", err)
		return true
	}
	if err := s.rdb.SetEx(ctx, s.recordPrefix+id, string(blob), s.recordTTL).Err(); err != nil {
		s.logger.Error("approval record write failed; entry kept for retry", "approval_id", id, "error", err)
		return false
	}
	s.logger.Info("approval task created", "approval_id", id, "approvers", approvers, "strategy", strategy)
	return true
}

// SubmitApprovalDecision records one approver's decision and — when the
// strategy threshold is met — publishes the approval_decision resume event.
// Serialized per approval by the SET NX decision lock (Python
// _submit_decision_redis): the record update is a multi-step read-verify-
// append-write, and concurrent submissions from multiple instances would
// otherwise clobber each other.
func (s *ApprovalSource) SubmitApprovalDecision(ctx context.Context, approvalID, approverID, decision, comments string) DecisionResult {
	lockKey := s.lockPrefix + approvalID
	holder := fmt.Sprintf("%d:%s", time.Now().UnixNano(), uuid.NewString())
	deadline := time.Now().Add(s.lockWait)
	for {
		ok, err := s.rdb.SetNX(ctx, lockKey, holder, s.lockTTL).Result()
		if err == nil && ok {
			break
		}
		if time.Now().After(deadline) {
			return DecisionResult{Status: "error", Message: "审批处理中，请稍后重试"}
		}
		time.Sleep(s.lockPoll)
	}
	defer func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := s.rdb.Eval(bgCtx, releaseIfOwnedLua, []string{lockKey}, holder).Result(); err != nil {
			s.logger.Warn("approval lock release failed (TTL reclaims)", "approval_id", approvalID, "error", err)
		}
	}()

	raw, err := s.rdb.Get(ctx, s.recordPrefix+approvalID).Result()
	if err != nil {
		return DecisionResult{Status: "error", Message: "审批任务不存在"}
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		s.logger.Error("approval record corrupt", "approval_id", approvalID, "error", err)
		return DecisionResult{Status: "error", Message: "审批任务不存在"}
	}
	taskCfg, _ := record["task_config"].(map[string]any)

	// Permission + duplicate checks (shared record ⇒ cross-instance).
	allowed := false
	for _, a := range toSlice(record["required_approvers"]) {
		if str(a) == approverID {
			allowed = true
			break
		}
	}
	if !allowed {
		return DecisionResult{Status: "error", Message: "无审批权限"}
	}
	approvals, _ := record["approvals"].([]any)
	for _, a := range approvals {
		if m, ok := a.(map[string]any); ok && str(m["approver_id"]) == approverID {
			return DecisionResult{Status: "error", Message: "已经审批过"}
		}
	}

	approvals = append(approvals, map[string]any{
		"approver_id": approverID,
		"decision":    decision,
		"comments":    comments,
		"timestamp":   time.Now().UnixMilli(),
	})
	record["approvals"] = approvals

	final := approvalFinalDecision(str(record["strategy"]), toSlice(record["required_approvers"]), approvals)
	if final == "" {
		blob, _ := json.Marshal(record)
		if err := s.rdb.SetEx(ctx, s.recordPrefix+approvalID, string(blob), s.recordTTL).Err(); err != nil {
			s.logger.Error("approval record update failed", "approval_id", approvalID, "error", err)
			return DecisionResult{Status: "error", Message: err.Error()}
		}
		s.logger.Info("approval decision recorded (pending threshold)", "approval_id", approvalID, "count", len(approvals))
		return DecisionResult{Status: "success", Message: "审批已记录，等待其他审批人"}
	}

	// Threshold met: publish the resume event, mark terminal, delete the
	// record (Python Redis path: terminal state moves to the execution row).
	eventData := map[string]any{
		"node_id":          taskCfg["node_id"],
		"execution_id":     taskCfg["execution_id"],
		"flow_id":          taskCfg["flow_id"],
		"tenant_id":        strOr(taskCfg["tenant_id"], s.cfg.DefaultTenantID),
		"trigger_type":     "approval_completed",
		"approval_id":      approvalID,
		"final_decision":   final,
		"approval_details": approvals,
		"timestamp":        time.Now().UnixMilli(),
		"success":          true,
	}
	tenant := strOr(taskCfg["tenant_id"], s.cfg.DefaultTenantID)
	ev := &worker.Event{
		EventID:       uuid.NewString(),
		EventType:     str(taskCfg["event_type"]),
		Data:          eventData,
		Timestamp:     float64(time.Now().UnixNano()) / 1e9, // epoch seconds (Python Event.timestamp convention)
		CorrelationID: str(taskCfg["execution_id"]),
		TenantID:      tenant,
	}
	if err := s.publishFn(ctx, ev); err != nil {
		// Trigger not confirmed: the record stays untouched (the failed
		// decision is NOT persisted — persisting it would wedge the approval:
		// the retry would hit 已经审批过 with the threshold already met but
		// no event ever published). The user retries the same decision, the
		// record re-appends and re-publishes — Python semantics verbatim.
		s.logger.Error("approval resume event publish failed; record untouched for retry",
			"approval_id", approvalID, "error", err)
		return DecisionResult{Status: "error", Message: err.Error()}
	}
	record["status"] = final
	record["completed_time"] = time.Now().UnixMilli()
	_ = s.rdb.Del(ctx, s.recordPrefix+approvalID).Err()
	s.logger.Info("approval completed", "approval_id", approvalID, "final_decision", final)
	return DecisionResult{Status: "success", FinalDecision: final, Message: "审批完成"}
}

// approvalFinalDecision mirrors Python _check_approval_result: any / all /
// majority over the required approvers; "" = threshold not met yet.
func approvalFinalDecision(strategy string, requiredApprovers []any, approvals []any) string {
	approveCount, rejectCount := 0, 0
	for _, a := range approvals {
		if m, ok := a.(map[string]any); ok {
			switch str(m["decision"]) {
			case "approve":
				approveCount++
			case "reject":
				rejectCount++
			}
		}
	}
	total := len(requiredApprovers)
	switch strategy {
	case "all":
		if rejectCount > 0 {
			return "reject"
		}
		if approveCount == total {
			return "approve"
		}
	case "majority":
		if rejectCount > total/2 {
			return "reject"
		}
		if approveCount > total/2 {
			return "approve"
		}
	default: // "any"
		if approveCount > 0 {
			return "approve"
		}
		if rejectCount > 0 {
			return "reject"
		}
	}
	return ""
}

func toSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

func strOr(v any, def string) string {
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return def
}
