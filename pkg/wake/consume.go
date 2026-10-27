package wake

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/worker"
)

// listSource is the shared list-queue consume core for the standard wake
// families (approval, redis_queue) — the Go counterpart of Python
// WakeConsumeMixin (wake.py): LRANGE batch read → parse/validate → in-flight
// dedup → async handle. The queue entry is LREM'd only when the handler
// confirms; it stays parked on publish failure or shutdown (at-least-once —
// the event-side NX dedup converges duplicate triggers), and bad entries are
// dropped in place so they can't wedge the queue.
//
// Long-running handlers (redis_queue listeners) keep their entry un-acked for
// the whole listen: a service crash mid-listen leaves the entry for the next
// start to re-consume, exactly like the Python skeleton.
type listSource struct {
	name         string // service_type = registry key segment = {prefix}:{name}:queue subtype
	rdb          *redis.Client
	queueKey     string
	batch        int
	pollInterval time.Duration
	logger       *slog.Logger

	// validate rejects (and drops) undecodable or non-conforming entries.
	validate func(task map[string]any) bool
	// preCheck returns false when the entry is already handled (dedup) and
	// should be acked without running (approval's record-exists check).
	preCheck func(ctx context.Context, task map[string]any) bool
	// handle runs the task; true = confirmed (ack), false = trigger failure
	// (entry stays for the next cycle). ctx cancellation inside handle means
	// shutdown: the entry stays and no attempt is counted.
	handle func(ctx context.Context, raw string, task map[string]any) bool

	publishFn func(context.Context, *worker.Event) error
	reg       *ServiceRegistry

	inflightMu sync.Mutex
	inflight   map[string]struct{}
	wg         sync.WaitGroup
	stopOnce   sync.Once
	done       chan struct{}
}

func newListSource(name string, cfg *Config, rdb *redis.Client) *listSource {
	return &listSource{
		name:         name,
		rdb:          rdb,
		queueKey:     cfg.KeyPrefix + ":" + name + ":queue",
		batch:        50,
		pollInterval: cfg.PollInterval,
		logger:       cfg.Logger,
		inflight:     make(map[string]struct{}),
		done:         make(chan struct{}),
		reg:          NewServiceRegistry(rdb, cfg, name),
	}
}

// Source reports the wake source identity.
func (s *listSource) Source() string { return s.name }

// Run drives the registry heartbeats + the consume loop until ctx is done.
func (s *listSource) Run(ctx context.Context) error {
	if err := s.reg.Beat(ctx, RegStatusStarting, 0); err != nil {
		s.logger.Warn("wake registry start beat failed; continuing (TTL roll-call will show the gap)",
			"source", s.name, "error", err)
	}
	regCtx, cancelReg := context.WithCancel(context.Background())
	regDone := make(chan struct{})
	go func() { s.registryLoop(regCtx); close(regDone) }()
	s.logger.Info("wake source started", "source", s.name, "queue", s.queueKey,
		"instance", s.reg.cfg.InstanceID)

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			cancelReg()
			<-regDone
			s.wg.Wait()
			s.reg.Stop(context.Background())
			s.stopOnce.Do(func() { close(s.done) })
			s.logger.Info("wake source stopped", "source", s.name, "queue", s.queueKey)
			return nil
		case <-ticker.C:
			s.cycleOnce(ctx)
		}
	}
}

// Done closes when the source has fully stopped (registry tombstoned).
func (s *listSource) Done() <-chan struct{} { return s.done }

func (s *listSource) registryLoop(ctx context.Context) {
	if err := s.reg.Beat(ctx, RegStatusRunning, 0); err != nil {
		s.logger.Warn("wake registry beat failed", "source", s.name, "error", err)
	}
	ticker := time.NewTicker(s.reg.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.reg.Beat(ctx, RegStatusRunning, s.activeCount()); err != nil {
				s.logger.Warn("wake registry beat failed", "source", s.name, "error", err)
			}
		}
	}
}

// cycleOnce is one consume round (Python _consume_wake_batch): read a batch,
// drop bad entries, dedup, dispatch handlers asynchronously.
func (s *listSource) cycleOnce(ctx context.Context) {
	raws, err := s.rdb.LRange(ctx, s.queueKey, 0, int64(s.batch)-1).Result()
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Error("wake consume read failed", "source", s.name, "error", err)
		}
		return
	}
	for _, raw := range raws {
		var task map[string]any
		if err := json.Unmarshal([]byte(raw), &task); err != nil || task == nil {
			s.logger.Error("wake task invalid JSON (dropped)", "source", s.name, "raw", truncate(raw, 200), "error", err)
			s.ack(ctx, raw)
			continue
		}
		if s.validate != nil && !s.validate(task) {
			s.logger.Error("wake task failed validation (dropped)", "source", s.name, "raw", truncate(raw, 200))
			s.ack(ctx, raw)
			continue
		}
		if s.preCheck != nil && !s.preCheck(ctx, task) {
			s.logger.Info("wake task already handled (duplicate dispatch, acked)", "source", s.name)
			s.ack(ctx, raw)
			continue
		}
		if !s.inflightTryAdd(raw) {
			continue
		}
		s.wg.Add(1)
		go func(raw string) {
			defer s.wg.Done()
			defer s.inflightDiscard(raw)
			confirmed := s.handle(ctx, raw, task)
			if ctx.Err() != nil {
				return // shutdown: entry stays parked, no attempt counted
			}
			if confirmed {
				s.ack(ctx, raw)
			} else {
				s.logger.Error("wake task trigger not confirmed; entry kept for retry",
					"source", s.name, "task", truncate(raw, 200))
			}
		}(raw)
	}
}

func (s *listSource) ack(ctx context.Context, raw string) {
	if err := s.rdb.LRem(ctx, s.queueKey, 1, raw).Err(); err != nil {
		s.logger.Error("wake task ack failed; a duplicate consume may follow (dedup converges)",
			"source", s.name, "error", err)
	}
}

func (s *listSource) inflightTryAdd(raw string) bool {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	if _, ok := s.inflight[raw]; ok {
		return false
	}
	s.inflight[raw] = struct{}{}
	return true
}

func (s *listSource) inflightDiscard(raw string) {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	delete(s.inflight, raw)
}

func (s *listSource) activeCount() int {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	return len(s.inflight)
}
