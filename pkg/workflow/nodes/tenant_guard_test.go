package nodes

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// The tenant gate (pn#18 / A42-06) on the three host-subprocess nodes: a
// non-default tenant must be refused BEFORE any spawn (the sentinel file
// proves zero execution chance), the refusal is deterministic and carries the
// public marker, dry runs stay allowed (nothing executes), and the default
// tenant keeps the historical semantics.

// requireTenantRefusalError checks the shared refusal shape (family sentinel
// error, public marker, named tenant, deterministic). Gates whose zero-
// execution proof is not a single file (redis state, git history) reuse this
// and assert their own side-effect sentinels.
func requireTenantRefusalError(t *testing.T, err error, nodeType string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s ran for a non-default tenant; want the tenant refusal", nodeType)
	}
	if !errors.Is(err, ErrTenantIsolationUnavailable) {
		t.Fatalf("%s error = %v, want ErrTenantIsolationUnavailable", nodeType, err)
	}
	if !strings.Contains(err.Error(), TenantIsolationMarker) {
		t.Errorf("%s error %q must carry the marker %q", nodeType, err, TenantIsolationMarker)
	}
	if !strings.Contains(err.Error(), `"acme"`) {
		t.Errorf("%s error %q must name the refused tenant", nodeType, err)
	}
	if workflow.IsRetryableError(err) {
		t.Errorf("%s tenant refusal must be deterministic (NonRetryable), got retryable", nodeType)
	}
}

func assertTenantRefusal(t *testing.T, err error, sentinel, nodeType string) {
	t.Helper()
	if err == nil {
		_, statErr := os.Stat(sentinel)
		t.Fatalf("%s ran for a non-default tenant (sentinel exists=%v); want the tenant refusal",
			nodeType, statErr == nil)
	}
	requireTenantRefusalError(t, err, nodeType)
	if _, statErr := os.Stat(sentinel); !os.IsNotExist(statErr) {
		t.Fatalf("%s command executed for a non-default tenant (sentinel %s exists)", nodeType, sentinel)
	}
}

func TestGateRefusesNonDefaultTenantBeforeSpawn(t *testing.T) {
	sentinel := filepath.Join(t.TempDir(), "gate-ran")
	node := &Gate{
		BaseNode: BaseNode{ID: "g1"},
		Command:  []any{"sh", "-c", "touch " + sentinel},
	}
	ctx := runtime.WithTenantID(gateTestCtx(t, map[string]any{}, nil), "acme")
	_, err := node.Execute(ctx)
	assertTenantRefusal(t, err, sentinel, "gate")
}

func TestGateDefaultTenantStillRuns(t *testing.T) {
	for _, tenant := range []string{"", runtime.DefaultTenantID} {
		sentinel := filepath.Join(t.TempDir(), "gate-ran")
		node := &Gate{
			BaseNode: BaseNode{ID: "g1"},
			Command:  []any{"sh", "-c", "touch " + sentinel},
		}
		ctx := runtime.WithTenantID(gateTestCtx(t, map[string]any{}, nil), tenant)
		res, err := node.Execute(ctx)
		if err != nil {
			t.Fatalf("tenant %q: Execute: %v", tenant, err)
		}
		if passed := res.Result.(map[string]any)["passed"]; passed != true {
			t.Errorf("tenant %q: passed = %v", tenant, passed)
		}
		if _, err := os.Stat(sentinel); err != nil {
			t.Fatalf("tenant %q: gate command must run for the default tenant: %v", tenant, err)
		}
	}
}

// Dry runs never execute a command, so the Python node lets them through for
// any tenant (gate.py checks dry before refuse_non_default_tenant).
func TestGateDryRunAllowedForNonDefaultTenant(t *testing.T) {
	sentinel := filepath.Join(t.TempDir(), "gate-ran")
	node := &Gate{
		BaseNode: BaseNode{ID: "g1"},
		Command:  []any{"sh", "-c", "touch " + sentinel},
		DryRun:   true,
	}
	ctx := runtime.WithTenantID(gateTestCtx(t, map[string]any{}, nil), "acme")
	res, err := node.Execute(ctx)
	if err != nil {
		t.Fatalf("dry-run gate under a non-default tenant: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["dry_run"] != true || out["passed"] != true {
		t.Errorf("dry-run result = %+v", out)
	}
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Fatalf("dry-run gate executed the command (sentinel exists)")
	}
}

func TestCaptureRefusesNonDefaultTenantBeforeSpawn(t *testing.T) {
	sentinel := filepath.Join(t.TempDir(), "capture-ran")
	node := &Capture{
		BaseNode: BaseNode{ID: "c1"},
		Command:  []any{"sh", "-c", "touch " + sentinel},
	}
	ctx := runtime.WithTenantID(gateTestCtx(t, map[string]any{}, nil), "acme")
	_, err := node.Execute(ctx)
	assertTenantRefusal(t, err, sentinel, "capture")
}

func TestCaptureDefaultTenantStillRuns(t *testing.T) {
	for _, tenant := range []string{"", runtime.DefaultTenantID} {
		sentinel := filepath.Join(t.TempDir(), "capture-ran")
		node := &Capture{
			BaseNode: BaseNode{ID: "c1"},
			Command:  []any{"sh", "-c", "touch " + sentinel},
		}
		ctx := runtime.WithTenantID(gateTestCtx(t, map[string]any{}, nil), tenant)
		res, err := node.Execute(ctx)
		if err != nil {
			t.Fatalf("tenant %q: Execute: %v", tenant, err)
		}
		if ok := res.Result.(map[string]any)["ok"]; ok != true {
			t.Errorf("tenant %q: ok = %v", tenant, ok)
		}
		if _, err := os.Stat(sentinel); err != nil {
			t.Fatalf("tenant %q: capture command must run for the default tenant: %v", tenant, err)
		}
	}
}

func TestCaptureDryRunAllowedForNonDefaultTenant(t *testing.T) {
	sentinel := filepath.Join(t.TempDir(), "capture-ran")
	node := &Capture{
		BaseNode: BaseNode{ID: "c1"},
		Command:  []any{"sh", "-c", "touch " + sentinel},
		DryRun:   true,
	}
	ctx := runtime.WithTenantID(gateTestCtx(t, map[string]any{}, nil), "acme")
	res, err := node.Execute(ctx)
	if err != nil {
		t.Fatalf("dry-run capture under a non-default tenant: %v", err)
	}
	if out := res.Result.(map[string]any); out["dry_run"] != true {
		t.Errorf("dry-run result = %+v", out)
	}
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Fatalf("dry-run capture executed the command (sentinel exists)")
	}
}

// pySentinelCode is user code whose `run` proves execution with a sentinel.
func pySentinelCode(sentinel string) string {
	return "def run(input):\n    open(" + strconv.Quote(sentinel) + ", 'w').write('ran')\n    return 'ok'\n"
}

func TestCodePythonRefusesNonDefaultTenantBeforeSpawn(t *testing.T) {
	pyRequirePython3(t)
	sentinel := filepath.Join(t.TempDir(), "python-ran")
	node := &Code{
		BaseNode: BaseNode{ID: "p1"},
		Language: LanguagePython,
		Code:     pySentinelCode(sentinel),
	}
	ctx := runtime.WithTenantID(context.Background(), "acme")
	_, err := node.Execute(ctx)
	assertTenantRefusal(t, err, sentinel, "code python")
}

func TestCodePythonDefaultTenantStillRuns(t *testing.T) {
	pyRequirePython3(t)
	for _, tenant := range []string{"", runtime.DefaultTenantID} {
		sentinel := filepath.Join(t.TempDir(), "python-ran")
		node := &Code{
			BaseNode: BaseNode{ID: "p1"},
			Language: LanguagePython,
			Code:     pySentinelCode(sentinel),
		}
		ctx := runtime.WithTenantID(context.Background(), tenant)
		if _, err := node.Execute(ctx); err != nil {
			t.Fatalf("tenant %q: Execute: %v", tenant, err)
		}
		if _, err := os.Stat(sentinel); err != nil {
			t.Fatalf("tenant %q: python sidecar must run for the default tenant: %v", tenant, err)
		}
	}
}

// The refusal sits in runPythonSubprocessBudget, the single spawn choke point,
// so a direct caller (bypassing the node) is gated too.
func TestPythonSidecarBudgetSeamRefusesNonDefaultTenant(t *testing.T) {
	pyRequirePython3(t)
	sentinel := filepath.Join(t.TempDir(), "python-ran")
	ctx := runtime.WithTenantID(context.Background(), "acme")
	_, err := runPythonSubprocessBudget(ctx, pySentinelCode(sentinel), nil,
		5*time.Second, 0, pyTestEnv(t))
	assertTenantRefusal(t, err, sentinel, "code python")
}
