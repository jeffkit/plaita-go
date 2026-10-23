package duration

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestParseISO8601(t *testing.T) {
	tests := []struct {
		in      string
		want    time.Duration
		wantSet bool
		wantErr bool
	}{
		{"PT0S", 0, true, false}, // explicit zero budget
		{"PT", 0, true, false},   // explicit zero budget (isodate accepts)
		{"PT0.3S", 300 * time.Millisecond, true, false},
		{"PT1.5S", 1500 * time.Millisecond, true, false},
		{"PT3S", 3 * time.Second, true, false},
		{"PT1M30S", 90 * time.Second, true, false},
		{"PT1H", time.Hour, true, false},
		{"P1D", 24 * time.Hour, true, false},
		{"P1W", 7 * 24 * time.Hour, true, false},
		{"P1DT0.5H", 24*time.Hour + 30*time.Minute, true, false},
		{"P1Y", 0, false, true}, // year/month have no fixed length
		{"P2M", 0, false, true},
		{"-PT1S", 0, false, true},
		{"soon", 0, false, true},
		{"", 0, false, true},
	}
	for _, tt := range tests {
		got, err := ParseISO8601(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseISO8601(%q) expected error", tt.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseISO8601(%q) unexpected error: %v", tt.in, err)
			continue
		}
		if got.TimeDuration() != tt.want {
			t.Errorf("ParseISO8601(%q) = %v, want %v", tt.in, got.TimeDuration(), tt.want)
		}
		if got.IsZero() != !tt.wantSet {
			t.Errorf("ParseISO8601(%q) IsZero() = %v, want set=%v", tt.in, got.IsZero(), tt.wantSet)
		}
	}
}

// "PT0S" is a set zero budget — the strictest limit — and must not be
// confused with "not configured" (the zero value).
func TestZeroBudgetIsNotZeroValue(t *testing.T) {
	var unset Duration
	requireTrue(t, unset.IsZero(), "zero value must read as unset")

	zero := MustParseDuration("PT0S")
	requireFalse(t, zero.IsZero(), "PT0S is configured")
	requireTrue(t, zero.TimeDuration() == 0, "PT0S budget is zero")
}

func TestMarshalRoundTrip(t *testing.T) {
	b, err := json.Marshal(MustParseDuration("PT1.5S"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"PT1.5S"` {
		t.Fatalf("marshal = %s, want %q", b, `"PT1.5S"`)
	}
	var back Duration
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.TimeDuration() != 1500*time.Millisecond {
		t.Fatalf("round trip = %v", back.TimeDuration())
	}

	if b, err = json.Marshal(Duration{}); err != nil {
		t.Fatal(err)
	}
	if string(b) != `"P0D"` {
		t.Fatalf("marshal unset = %s", b)
	}
	if err := json.Unmarshal([]byte(`null`), &back); err != nil {
		t.Fatalf("null: %v", err)
	}
	if !back.IsZero() {
		t.Fatal("null must leave the duration unset")
	}
	if err := json.Unmarshal([]byte(`300`), &back); err == nil {
		t.Fatal("engine JSON spells timeouts as ISO strings; a bare number must fail")
	}
}

// #57: the int64-nanosecond synthesis has no natural overflow check —
// declared budgets past time.Duration's ~292-year domain used to wrap:
// negative (instant timeout), zero, or a silently shortened positive. All
// shapes of the same overflow must fail closed with the named
// representability error (same shape as plaita-rs duration.rs).
func TestParseISO8601OverflowIsNamedError(t *testing.T) {
	cases := []string{
		"P106752D",              // wraps negative (issue probe: instant deadline)
		"P281474976710656D",     // wraps to 0 (issue probe: reads as PT0S)
		"P213504D",              // wraps to a shortened positive (~25min)
		"P1317624576693539402W", // weeks*7 overflows int64 before the day fold
		"PT9223372037S",         // seconds past the int64-ns domain (float->int wraps negative on amd64)
		"P106751DT23H59M59S",    // every part in range, total over the domain
	}
	for _, in := range cases {
		d, err := ParseISO8601(in)
		if err == nil {
			t.Errorf("ParseISO8601(%q) = %s, want the named not-representable error", in, d)
			continue
		}
		if !strings.Contains(err.Error(), "not representable") {
			t.Errorf("ParseISO8601(%q) error = %q, want it to name the unrepresentable value", in, err)
		}
	}
	// the biggest day-grain budget that still fits int64 nanoseconds keeps
	// parsing with a positive value
	d, err := ParseISO8601("P106751D")
	if err != nil || d.TimeDuration() <= 0 {
		t.Errorf("ParseISO8601(P106751D) = %v, %v; want success with a positive duration", d, err)
	}
}

// Every parseable duration must read as a non-negative budget — the wrapped
// negative form is the regression red line (#57).
func TestParseISO8601NeverNegative(t *testing.T) {
	for _, in := range []string{"PT24H", "PT86400S", "PT0S", "PT", "P1D", "P1W", "P1DT0.5H", "P106751D", "PT0.3S", "PT9223372036S"} {
		d, err := ParseISO8601(in)
		if err != nil {
			t.Fatalf("ParseISO8601(%q): %v", in, err)
		}
		if d.TimeDuration() < 0 {
			t.Errorf("ParseISO8601(%q) = %s: a negative budget must be impossible", in, d)
		}
	}
}

func requireTrue(t *testing.T, got bool, msg string) {
	t.Helper()
	if !got {
		t.Fatal(msg)
	}
}

func requireFalse(t *testing.T, got bool, msg string) {
	t.Helper()
	if got {
		t.Fatal(msg)
	}
}
