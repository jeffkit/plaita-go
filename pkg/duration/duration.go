// Package duration provides time duration
package duration

import (
	"time"

	"github.com/senseyeio/duration"
)

// Duration represents a duration for ISO 8601 duration format
type Duration struct {
	duration.Duration
}

// MustParseDuration must parse a duration string
func MustParseDuration(s string) Duration {
	d, err := duration.ParseISO8601(s)
	if err != nil {
		panic(err)
	}
	return Duration{d}
}

// TimeDuration returns time.Duration
func (d Duration) TimeDuration() time.Duration {
	return time.Duration(d.TH)*time.Hour + time.Duration(d.TM)*time.Minute + time.Duration(d.TS)*time.Second
}
