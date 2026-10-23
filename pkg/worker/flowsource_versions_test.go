package worker

import (
	"context"
	"fmt"
	"testing"
)

// #61: empty-version resolution must pick the numerically-largest version by
// dotted-tuple order (Python [int(p) for p in x.split(".")]). The old
// comparator fed dotted versions to ParseFloat ("1.10" == 1.1 < "1.9") and
// fell back to string compare for three-segment versions ("9.0.0" >
// "10.0.0") — silently executing the wrong definition.

func seedVersionSet(t *testing.T, w *Worker, flowID string, versions []string) {
	t.Helper()
	members := make([]any, 0, len(versions))
	for _, v := range versions {
		members = append(members, v)
	}
	if err := w.flows.client.SAdd(context.Background(), w.flows.versionsKey("", flowID), members...).Err(); err != nil {
		t.Fatal(err)
	}
}

// resolveVersionOf resolves the empty version for a seeded set, failing the
// test on transport errors.
func resolveVersionOf(t *testing.T, w *Worker, flowID string) string {
	t.Helper()
	got, err := w.flows.resolveVersion(context.Background(), "", flowID, "")
	if err != nil {
		t.Fatalf("resolveVersion(%s): %v", flowID, err)
	}
	return got
}

// 对拍表: the five shapes from the issue plus the v-prefix and numeric-vs-
// non-numeric filter edges. Empty want = "any member of the set" (Python
// next(iter(all_versions))) — asserted as membership, not a pinned value.
func TestResolveVersionDottedTupleParity(t *testing.T) {
	w, _, _, _ := newTestWorker(t)
	cases := []struct {
		name     string
		versions []string
		want     string
	}{
		{"two-segment float collapse", []string{"1.9", "1.10"}, "1.10"},
		{"three-segment lexicographic", []string{"1.10.2", "1.10.10"}, "1.10.10"},
		{"cross-major lexicographic", []string{"1.9.0", "1.10.0", "9.0.0", "10.0.0"}, "10.0.0"},
		{"single-segment numeric", []string{"2", "11"}, "11"},
		{"all non-numeric", []string{"preview", "beta"}, ""},
		{"v-prefix not a numeric version", []string{"v1.2", "1.3"}, "1.3"},
		{"numeric beats non-numeric", []string{"1.10", "zzz"}, "1.10"},
		{"shorter tuple prefix sorts first", []string{"1.10", "1.10.0"}, "1.10.0"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flowID := fmt.Sprintf("ver_flow_%d", i)
			seedVersionSet(t, w, flowID, tc.versions)
			got := resolveVersionOf(t, w, flowID)
			if tc.want == "" {
				if !contains(tc.versions, got) {
					t.Fatalf("resolveVersion(%v) = %q, want any member of the set (no panic)", tc.versions, got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("resolveVersion(%v) = %q, want %q (dotted tuple order)", tc.versions, got, tc.want)
			}
		})
	}
}

// End-to-end: the empty-version start path (worker.go GetFlowDefinition) must
// land on the numeric max, not the float/lexicographic survivor.
func TestFlowSourceEmptyVersionPicksNumericMax(t *testing.T) {
	w, _, _, _ := newTestWorker(t)
	ctx := context.Background()
	for _, v := range []string{"1.9", "1.10"} {
		if err := w.flows.PresetFlowDefinition(ctx, "", "dot_flow", v, mustHelloWorld(t)); err != nil {
			t.Fatal(err)
		}
	}
	def, err := w.flows.GetFlowDefinition(ctx, "", "dot_flow", "")
	if err != nil {
		t.Fatalf("resolve empty version: %v", err)
	}
	if def.Version != "1.10" {
		t.Fatalf("empty-version resolve = %q, want 1.10 (numeric tuple order, not float collapse)", def.Version)
	}
}
