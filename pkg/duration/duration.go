// Package duration provides ISO 8601 duration parsing for flow timeouts.
//
// The engine contract (plaita core runner _parse_timeout):
//
//   - unset / empty means "no timeout";
//   - an explicit zero ("PT0S", "PT") is a SET zero budget (strictest limit —
//     it times out immediately) and must not degrade to "no timeout";
//   - fractional seconds ("PT0.3S") and days ("P1D") are supported;
//   - year/month components are rejected: they have no fixed length and a
//     milliseconds budget would silently drop them.
package duration

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Duration represents an ISO 8601 duration with fixed-length components.
// The zero value means "not set" (no timeout configured); a parsed "PT0S"
// is a set, zero-length budget. IsZero reports "not set", which keeps the
// historical `if !d.IsZero()` call sites meaning "a timeout is configured".
type Duration struct {
	d   time.Duration
	set bool
}

var isoPattern = regexp.MustCompile(`^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?` +
	`(?:T(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?)?$`)

// ParseISO8601 parses an ISO 8601 duration string with fixed-length
// components. "PT" parses as an explicit zero budget.
func ParseISO8601(s string) (Duration, error) {
	m := isoPattern.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return Duration{}, fmt.Errorf("could not parse duration string %q", s)
	}
	// Year/month components have no fixed number of seconds (plaita rejects
	// them for the same reason: a milliseconds budget would silently drop
	// them, "P1Y" -> 0).
	if m[1] != "" || m[2] != "" {
		return Duration{}, fmt.Errorf("duration %q contains year/month components, which have no fixed length", s)
	}
	weeks, err := atoiComponent(m[3])
	if err != nil {
		return Duration{}, err
	}
	days, err := atoiComponent(m[4])
	if err != nil {
		return Duration{}, err
	}
	if weeks > math.MaxInt64/7 {
		return Duration{}, unrepresentable(s)
	}
	days += weeks * 7
	hours, err := atofComponent(m[5], s)
	if err != nil {
		return Duration{}, err
	}
	minutes, err := atofComponent(m[6], s)
	if err != nil {
		return Duration{}, err
	}
	seconds, err := atofComponent(m[7], s)
	if err != nil {
		return Duration{}, err
	}
	// The synthesis below is int64 nanoseconds with no natural overflow
	// check — a declared budget past time.Duration's ~292-year domain used
	// to wrap into a negative, zero, or shortened budget that only detonated
	// at execute time (#57). Bound the total in float64 before converting
	// (components are non-negative, so every per-component conversion then
	// stays in range and cannot wrap negative on either float->int behaviour:
	// amd64 wraps, arm64 saturates); the post-synthesis guard catches a
	// float-rounding slip in the last nanoseconds of the domain. Named
	// fail-closed shape aligned with plaita-rs duration.rs.
	if total := (float64(days)*86400 + hours*3600 + minutes*60 + seconds) * 1e9; total > float64(math.MaxInt64) {
		return Duration{}, unrepresentable(s)
	}
	d := time.Duration(days)*24*time.Hour +
		time.Duration(hours*float64(time.Hour)) +
		time.Duration(minutes*float64(time.Minute)) +
		time.Duration(seconds*float64(time.Second))
	if d < 0 {
		return Duration{}, unrepresentable(s)
	}
	return Duration{d: d, set: true}, nil
}

// unrepresentable reports a duration whose nanosecond total does not fit a
// time.Duration (#57).
func unrepresentable(s string) error {
	return fmt.Errorf("duration %q is not representable as a time.Duration", s)
}

// MustParseDuration must parse a duration string
func MustParseDuration(s string) Duration {
	d, err := ParseISO8601(s)
	if err != nil {
		panic(err)
	}
	return d
}

// FromTimeDuration wraps a time.Duration as a set Duration.
func FromTimeDuration(d time.Duration) Duration {
	return Duration{d: d, set: true}
}

// IsZero reports whether no duration was configured. An explicit "PT0S" is
// set and therefore NOT zero.
func (d Duration) IsZero() bool {
	return !d.set
}

// TimeDuration returns time.Duration
func (d Duration) TimeDuration() time.Duration {
	return d.d
}

// String returns an ISO 8601 representation ("P0D" when unset).
func (d Duration) String() string {
	if !d.set {
		return "P0D"
	}
	return "PT" + strconv.FormatFloat(d.d.Seconds(), 'f', -1, 64) + "S"
}

// MarshalJSON satisfies json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON satisfies json.Unmarshaler. The engine-native JSON spelling
// is an ISO 8601 string; null leaves the duration unset.
func (d *Duration) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" {
		*d = Duration{}
		return nil
	}
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return fmt.Errorf("timeout must be an ISO 8601 duration string, got %s", b)
	}
	parsed, err := ParseISO8601(str)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

func atoiComponent(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration component %q", s)
	}
	return v, nil
}

func atofComponent(s, whole string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsInf(v, 0) || math.IsNaN(v) {
		return 0, fmt.Errorf("invalid duration component in %q", whole)
	}
	return v, nil
}
