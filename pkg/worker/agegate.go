package worker

import (
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

// taskAgeFromStreamID parses a Redis stream id ("<ms>-<seq>") into an age in
// seconds. Only purely numeric ids carry a usable write time (Python
// timestamp_from_stream_id 口径).
func taskAgeFromStreamID(messageID string) (float64, bool) {
	dash := strings.IndexByte(messageID, '-')
	if dash <= 0 {
		return 0, false
	}
	ms, err := strconv.ParseInt(messageID[:dash], 10, 64)
	if err != nil || ms <= 0 {
		return 0, false
	}
	return time.Since(time.UnixMilli(ms)).Seconds(), true
}

// carrierAgeSeconds is the dual-source max age (payload timestamp vs Stream
// ID); known=false when neither source is determinable.
func carrierAgeSeconds(task *Task, messageID string) (age float64, known bool) {
	best := -1.0
	if a, ok := taskAgeFromTimestamp(task.Timestamp); ok && a > best {
		best = a
	}
	if a, ok := taskAgeFromStreamID(messageID); ok && a > best {
		best = a
	}
	if best < 0 {
		return 0, false
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
		// since before the horizon; admitting it would void the gate.
		w.logger.Warn("task age undeterminable (no timestamp, non-numeric stream id); dropping fail-closed",
			"type", task.Type, "id", messageID)
		w.metrics.IncCounter(MetricTaskAgeUnknownTotal, metricLabels("tenant", task.TenantID, "type", task.Type))
		return true
	}
	return age > maxAge
}
