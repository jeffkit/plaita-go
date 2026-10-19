package nodes

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// rateLimitTestCtx builds a runtime context for rate_limit execution.
func rateLimitTestCtx(t *testing.T) context.Context {
	t.Helper()
	return runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput:      map[string]any{},
		ExecuteNodeResults: map[string]any{},
		EnvVariables:       map[string]any{},
		GlobalVariables:    map[string]any{},
	})
}

func TestRateLimitCheckEmpty(t *testing.T) {
	node := &RateLimit{
		BaseNode: BaseNode{ID: "r1"},
		Key:      "xhs-publish",
		StoreDir: t.TempDir(),
	}
	res, err := node.Execute(rateLimitTestCtx(t))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["allowed"] != true {
		t.Errorf("allowed = %v", out["allowed"])
	}
	if out["reason"] != "日 0/1，周 0/5" {
		t.Errorf("reason = %v, want default limits 1/5", out["reason"])
	}
	if out["today_count"] != 0 || out["week_count"] != 0 {
		t.Errorf("counts = %v/%v, want 0/0", out["today_count"], out["week_count"])
	}
}

func TestRateLimitRecordAndWindows(t *testing.T) {
	dir := t.TempDir()
	base := time.Unix(1_700_000_000, 0)
	cur := base
	node := &RateLimit{
		BaseNode: BaseNode{ID: "r1"},
		Key:      "xhs-publish",
		StoreDir: dir,
		Action:   "record",
		Now:      func() time.Time { return cur },
	}

	res, err := node.Execute(rateLimitTestCtx(t)) // record
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["recorded"] != true || out["key"] != "xhs-publish" || out["total"] != 1 {
		t.Errorf("record output = %v", out)
	}

	cur = base.Add(2 * time.Hour)
	node.Action = "check"
	res, err = node.Execute(rateLimitTestCtx(t)) // check same day
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	out = res.Result.(map[string]any)
	if out["allowed"] != false {
		t.Errorf("second call same day must be denied (daily=1): %v", out)
	}
	if out["today_count"] != 1 || out["week_count"] != 1 {
		t.Errorf("counts = %v/%v, want 1/1", out["today_count"], out["week_count"])
	}

	cur = base.Add(25 * time.Hour)
	res, err = node.Execute(rateLimitTestCtx(t)) // check next day
	if err != nil {
		t.Fatalf("check next day: %v", err)
	}
	out = res.Result.(map[string]any)
	if out["allowed"] != true {
		t.Errorf("next-day call must be allowed (weekly=5): %v", out)
	}
	if out["today_count"] != 0 || out["week_count"] != 1 {
		t.Errorf("counts = %v/%v, want 0/1", out["today_count"], out["week_count"])
	}
}

func TestRateLimitCustomLimits(t *testing.T) {
	dir := t.TempDir()
	two := 2
	node := &RateLimit{
		BaseNode: BaseNode{ID: "r1"},
		Key:      "k",
		StoreDir: dir,
		Action:   "record",
		Daily:    &two,
		Weekly:   &two,
	}
	for i := 0; i < 2; i++ {
		if _, err := node.Execute(rateLimitTestCtx(t)); err != nil { // record x2
			t.Fatalf("record %d: %v", i, err)
		}
	}
	node.Action = "check"
	res, err := node.Execute(rateLimitTestCtx(t)) // check
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["allowed"] != false {
		t.Errorf("daily=2 reached, must be denied: %v", out)
	}
	if out["reason"] != "日 2/2，周 2/2" {
		t.Errorf("reason = %v", out["reason"])
	}
}

func TestRateLimitClear(t *testing.T) {
	node := &RateLimit{
		BaseNode: BaseNode{ID: "r1"},
		Key:      "k",
		StoreDir: t.TempDir(),
	}
	if _, err := node.Execute(rateLimitTestCtx(t)); err != nil { // record
		t.Fatalf("record: %v", err)
	}
	node.Action = "clear"
	res, err := node.Execute(rateLimitTestCtx(t))
	if err != nil {
		t.Fatalf("clear: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["cleared"] != true || out["key"] != "k" {
		t.Errorf("clear output = %v", out)
	}
	node.Action = "check"
	res, err = node.Execute(rateLimitTestCtx(t))
	if err != nil {
		t.Fatalf("check after clear: %v", err)
	}
	if out := res.Result.(map[string]any); out["today_count"] != 0 {
		t.Errorf("counter not cleared: %v", out)
	}
}

func TestRateLimitAcquire(t *testing.T) {
	node := &RateLimit{
		BaseNode: BaseNode{ID: "r1"},
		Key:      "k",
		StoreDir: t.TempDir(),
		Action:   "acquire",
	}
	res, err := node.Execute(rateLimitTestCtx(t))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	out := res.Result.(map[string]any)
	// acquire returns the post-booking counts in the check shape + recorded.
	if out["allowed"] != true || out["recorded"] != true {
		t.Errorf("first acquire = %v", out)
	}
	if out["today_count"] != 1 || out["week_count"] != 1 {
		t.Errorf("post-acquire counts = %v/%v, want 1/1", out["today_count"], out["week_count"])
	}
	if out["reason"] != "日 1/1，周 1/5" {
		t.Errorf("reason = %v", out["reason"])
	}

	res, err = node.Execute(rateLimitTestCtx(t))
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	out = res.Result.(map[string]any)
	if out["allowed"] != false || out["recorded"] != false {
		t.Errorf("second acquire must be denied: %v", out)
	}
}

func TestRateLimitAcquireConcurrentExactlyOne(t *testing.T) {
	node := &RateLimit{
		BaseNode: BaseNode{ID: "r1"},
		Key:      "k",
		StoreDir: t.TempDir(),
		Action:   "acquire",
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowedCount := 0
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := node.Execute(rateLimitTestCtx(t))
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			if res.Result.(map[string]any)["allowed"] == true {
				mu.Lock()
				allowedCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowedCount != 1 {
		t.Errorf("allowed = %d times, want exactly 1 (flock mutual exclusion)", allowedCount)
	}
}

func TestRateLimitKeyWhitelist(t *testing.T) {
	cases := []struct {
		key     string
		wantErr bool
	}{
		{"xhs-publish", false},
		{"Report_2026.1", false},
		{"../evil", true},
		{"a/b", true},
		{".hidden", true},
		{"..", true},
		{"/abs", true},
		{"a%2fb", true},
		{"with space", true},
		{"a:b", true},
	}
	for _, tc := range cases {
		node := &RateLimit{
			BaseNode: BaseNode{ID: "r1"},
			Key:      tc.key,
			StoreDir: t.TempDir(),
		}
		_, err := node.Execute(rateLimitTestCtx(t))
		if (err != nil) != tc.wantErr {
			t.Errorf("key %q: err = %v, wantErr %v", tc.key, err, tc.wantErr)
			continue
		}
		if err != nil && !strings.Contains(err.Error(), "非法") {
			t.Errorf("key %q: err = %v, want named whitelist error", tc.key, err)
		}
	}
}

func TestRateLimitKeyDefaultAndExpression(t *testing.T) {
	dir := t.TempDir()
	node := &RateLimit{ // no key → "default"
		BaseNode: BaseNode{ID: "r1"},
		StoreDir: dir,
		Action:   "record",
	}
	if _, err := node.Execute(rateLimitTestCtx(t)); err != nil {
		t.Fatalf("record: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "default.json")); err != nil {
		t.Errorf("default counter file missing: %v", err)
	}

	node = &RateLimit{
		BaseNode: BaseNode{ID: "r2"},
		Key:      "$INPUT.name",
		StoreDir: dir,
		Action:   "record",
	}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput:      map[string]any{"name": "expr-key"},
		ExecuteNodeResults: map[string]any{},
		GlobalVariables:    map[string]any{},
	})
	if _, err := node.Execute(ctx); err != nil {
		t.Fatalf("record expr key: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "expr-key.json")); err != nil {
		t.Errorf("expression key counter missing: %v", err)
	}
}

func TestRateLimitCorruptCounterTreatedEmpty(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "broken.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	node := &RateLimit{
		BaseNode: BaseNode{ID: "r1"},
		Key:      "broken",
		StoreDir: dir,
	}
	res, err := node.Execute(rateLimitTestCtx(t)) // check
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["allowed"] != true || out["today_count"] != 0 {
		t.Errorf("corrupt counter must degrade to empty: %v", out)
	}
}

func TestRateLimitStampTruncation(t *testing.T) {
	dir := t.TempDir()
	base := float64(1_700_000_000)
	stamps := make([]float64, 100)
	for i := range stamps {
		stamps[i] = base + float64(i)
	}
	data, _ := json.Marshal(stamps)
	if err := os.WriteFile(filepath.Join(dir, "k.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	node := &RateLimit{
		BaseNode: BaseNode{ID: "r1"},
		Key:      "k",
		StoreDir: dir,
		Action:   "record",
		Now:      func() time.Time { return time.Unix(int64(base)+1000, 0) },
	}
	res, err := node.Execute(rateLimitTestCtx(t))
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if out := res.Result.(map[string]any); out["total"] != 101 {
		t.Errorf("total = %v, want 101 (pre-truncation count)", out["total"])
	}
	stored, err := os.ReadFile(filepath.Join(dir, "k.json"))
	if err != nil {
		t.Fatal(err)
	}
	var kept []float64
	if err := json.Unmarshal(stored, &kept); err != nil {
		t.Fatalf("stored counter unparsable: %v", err)
	}
	if len(kept) != 100 {
		t.Errorf("stored stamps = %d, want capped 100", len(kept))
	}
	if kept[len(kept)-1] != base+1000 {
		t.Errorf("newest stamp missing: last = %v", kept[len(kept)-1])
	}
}

func TestRateLimitUnknownAction(t *testing.T) {
	node := &RateLimit{
		BaseNode: BaseNode{ID: "r1"},
		Key:      "k",
		StoreDir: t.TempDir(),
		Action:   "flush",
	}
	_, err := node.Execute(rateLimitTestCtx(t))
	if err == nil || !strings.Contains(err.Error(), "未知 rate_limit action: flush") {
		t.Fatalf("err = %v, want named unknown-action error", err)
	}
}

func TestRateLimitStoreDirExpression(t *testing.T) {
	dir := t.TempDir()
	node := &RateLimit{
		BaseNode: BaseNode{ID: "r1"},
		Key:      "k",
		StoreDir: "$INPUT.dir",
		Action:   "record",
	}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput:      map[string]any{"dir": dir},
		ExecuteNodeResults: map[string]any{},
		GlobalVariables:    map[string]any{},
	})
	if _, err := node.Execute(ctx); err != nil {
		t.Fatalf("record: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "k.json")); err != nil {
		t.Errorf("counter file missing from evaluated store_dir: %v", err)
	}
}

func TestRateLimitJSONRoundTripAndRegistration(t *testing.T) {
	doc := `{"type":"rate_limit","id":"r1","action":"acquire","key":"pub","daily":3,"weekly":7}`
	node := new(RateLimit)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.ID != "r1" || node.Action != "acquire" || node.Key != "pub" {
		t.Errorf("fields = %+v", node)
	}
	if node.Daily == nil || *node.Daily != 3 || node.Weekly == nil || *node.Weekly != 7 {
		t.Errorf("limits = %v/%v", node.Daily, node.Weekly)
	}
	if workflow.GetNodeFactory(nodeTypeRateLimit) == nil {
		t.Error("rate_limit node type not registered")
	}
}
