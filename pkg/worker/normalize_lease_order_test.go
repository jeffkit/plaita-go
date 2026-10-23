package worker

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// normalizeLeaseProbe is a client-side redis.Hook watching the §9.2
// normalization XADD (#55): after the command has executed it records the
// execution lease value seen at that instant and optionally holds the worker
// inside the post-XADD window so a second consumer can race the normalizer
// deterministically (no sleeps). All reads go through a separate client so
// the hook does not observe itself.
type normalizeLeaseProbe struct {
	leaseKey string
	probe    *redis.Client
	// leaseAtXAdd is the lease value at the normalization XADD instant
	// ("" = released).
	leaseAtXAdd string
	// xaddSeen closes after the normalization XADD has hit the stream.
	xaddSeen chan struct{}
	// hold, when non-nil, blocks the worker after the XADD until closed.
	hold chan struct{}
}

func (p *normalizeLeaseProbe) DialHook(next redis.DialHook) redis.DialHook { return next }

func (p *normalizeLeaseProbe) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (p *normalizeLeaseProbe) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if p.isNormalizeXAdd(cmd) {
			p.leaseAtXAdd = p.probe.Get(ctx, p.leaseKey).Val()
			if p.xaddSeen != nil {
				close(p.xaddSeen)
			}
			if p.hold != nil {
				select {
				case <-p.hold:
				case <-time.After(10 * time.Second):
					// fail-safe: never wedge the worker past the test's own
					// failure reporting
				}
			}
		}
		return err
	}
}

func (p *normalizeLeaseProbe) isNormalizeXAdd(cmd redis.Cmder) bool {
	if cmd.Name() != "xadd" {
		return false
	}
	for _, arg := range cmd.Args() {
		if s, ok := arg.(string); ok && strings.Contains(s, `"resume_type":"continue"`) {
			return true
		}
	}
	return false
}

// resumeEventOnRunning seeds a running row + an event carrier, the §9.2
// normalization trigger (#258).
func resumeEventOnRunning(t *testing.T, rdb *redis.Client, cfg *Config, executionID string) {
	t.Helper()
	rdb.Set(context.Background(), stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"status":"running"}`, executionID), 0)
	payload := baseStartPayload(executionID)
	payload["type"] = "resume"
	payload["resume_type"] = "event"
	enqueueTask(t, rdb, cfg, payload, "")
}

// TestNormalizedContinueEnqueuedAfterLeaseRelease (#55): the normalized
// continue carrier must enter the stream only after the normalizer released
// the execution lease — enqueueing under our own lease hands the new carrier
// to any concurrent consumer that hits the §3.5 live-holder rule and acks the
// only recovery path (same ordering discipline as routeStartRedeliveryOnRunning,
// the #47 fix).
func TestNormalizedContinueEnqueuedAfterLeaseRelease(t *testing.T) {
	w, rdb, mr, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()
	resumeEventOnRunning(t, rdb, cfg, executionID)

	probeRdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer probeRdb.Close()
	probe := &normalizeLeaseProbe{
		leaseKey: cfg.KeyPrefix + ":execution:lease:" + executionID,
		probe:    probeRdb,
	}
	rdb.AddHook(probe)

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("queue entries = %d, want 1 normalized carrier", n)
	}
	if probe.leaseAtXAdd != "" {
		t.Fatalf("normalization XADD executed while the lease was still held (%q) — "+
			"the new carrier races its own holder's §3.5 live-holder ack (#55)", probe.leaseAtXAdd)
	}
}

// TestNormalizedContinueSurvivesInterleavedConsumer (#55): two workers, the
// second consumes the normalized carrier inside the post-XADD window. With
// the lease still held at enqueue time the new carrier is acked under the
// §3.5 live-holder rule and the execution strands in running (stream 0, PEL 0,
// no terminal); after the release-before-enqueue ordering the second worker
// acquires cleanly and the execution reaches a named terminal.
func TestNormalizedContinueSurvivesInterleavedConsumer(t *testing.T) {
	w1, rdb, mr, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()
	resumeEventOnRunning(t, rdb, cfg, executionID)

	// ProcessOne flushes SetActiveTasks on every run; a worker whose registry
	// was never Start()ed would rewrite its own registry key with a blank
	// ServiceInfo (status ""), so give the normalizer a real running entry —
	// the §3.5 live-holder shape the issue describes.
	w1.registry.Start("")
	w1.registry.SetStatus(ctx, RegStatusRunning)

	probeRdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer probeRdb.Close()
	probe := &normalizeLeaseProbe{
		leaseKey: cfg.KeyPrefix + ":execution:lease:" + executionID,
		probe:    probeRdb,
		xaddSeen: make(chan struct{}),
		hold:     make(chan struct{}),
	}
	rdb.AddHook(probe)

	// Worker two: same shadow namespace, own client (its commands must not
	// pass the blocking hook) and own instance id (its un-Start()ed registry
	// flushes must not clobber the holder entry being probed).
	cfg2 := *cfg
	cfg2.InstanceID = "interleaver"
	rdb2 := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb2.Close()
	w2 := New(&cfg2, rdb2, w1.logger)

	done := make(chan error, 1)
	go func() {
		_, err := w1.ProcessOne(ctx)
		done <- err
	}()

	select {
	case <-probe.xaddSeen:
	case <-time.After(10 * time.Second):
		t.Fatal("normalization XADD never executed")
	}
	if _, err := w2.ProcessOne(ctx); err != nil {
		t.Fatalf("interleaving ProcessOne: %v", err)
	}
	close(probe.hold)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("normalizing ProcessOne: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("normalizing ProcessOne never returned")
	}

	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue entries = %d, want 0 (all carriers consumed)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0 (all carriers acked)", n)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status == StatusRunning {
		t.Fatalf("execution stranded in running with both carriers gone (#55): "+
			"lease at XADD = %q", probe.leaseAtXAdd)
	}
	if state.Status != StatusError || state.Error["error_type"] != "flow_not_found" {
		t.Fatalf("status = %s error = %v, want a named flow_not_found terminal from the "+
			"interleaving consumer advancing the execution", state.Status, state.Error)
	}
}
