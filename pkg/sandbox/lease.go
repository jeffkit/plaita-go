package sandbox

import (
	"crypto/sha1"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// LeaseStore is the storage contract for workspace leases. FileLeaseStore is
// the single-host default; a multi-worker deployment can inject an
// implementation with the same SET NX EX semantics (the Python driver's
// Redis store is the reference).
type LeaseStore interface {
	Acquire(key, holder string, ttl time.Duration) (bool, error)
	Renew(key, holder string, ttl time.Duration) (bool, error)
	Release(key, holder string) (bool, error)
}

// FileLeaseStore uses O_EXCL for atomic acquisition and expires stale leases
// by reading the payload's expires_at. It matches sandbox.py's
// FileLeaseStore: an expired lease may be pre-empted once; live leases fail
// fast.
type FileLeaseStore struct {
	Dir string
}

// DefaultLeaseStore roots leases under PLAITA_HOME/sandbox-leases (another
// shadow-invisible path; no production Redis keys are touched).
func DefaultLeaseStore() *FileLeaseStore {
	return &FileLeaseStore{Dir: filepath.Join(DefaultHome(), "sandbox-leases")}
}

func (s *FileLeaseStore) path(key string) string {
	sum := sha1.Sum([]byte(key))
	return filepath.Join(s.Dir, hex.EncodeToString(sum[:])+".lease")
}

type leasePayload struct {
	Holder    string  `json:"holder"`
	ExpiresAt float64 `json:"expires_at"`
}

func (s *FileLeaseStore) read(path string) (*leasePayload, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var payload leasePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func (s *FileLeaseStore) write(path, holder string, ttl time.Duration) error {
	payload, err := json.Marshal(leasePayload{
		Holder:    holder,
		ExpiresAt: float64(time.Now().Add(ttl).UnixNano()) / 1e9,
	})
	if err != nil {
		return err
	}
	return os.WriteFile(path, payload, 0o600)
}

// Acquire implements the O_EXCL + expired-pre-emption loop.
func (s *FileLeaseStore) Acquire(key, holder string, ttl time.Duration) (bool, error) {
	if err := os.MkdirAll(s.Dir, 0o700); err != nil {
		return false, fmt.Errorf("sandbox lease dir: %w", err)
	}
	path := s.path(key)
	for attempt := 0; attempt < 2; attempt++ {
		fd, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			payload, marshalErr := json.Marshal(leasePayload{
				Holder:    holder,
				ExpiresAt: float64(time.Now().Add(ttl).UnixNano()) / 1e9,
			})
			if marshalErr != nil {
				_ = fd.Close()
				_ = os.Remove(path)
				return false, marshalErr
			}
			_, writeErr := fd.Write(payload)
			closeErr := fd.Close()
			if writeErr != nil || closeErr != nil {
				_ = os.Remove(path)
				if writeErr != nil {
					return false, writeErr
				}
				return false, closeErr
			}
			return true, nil
		}
		if !os.IsExist(err) {
			return false, err
		}
		payload, readErr := s.read(path)
		if readErr != nil {
			// Unreadable/corrupt lease: do not steal it. Operator action.
			return false, nil
		}
		if payload.ExpiresAt > float64(time.Now().UnixNano())/1e9 {
			return false, nil
		}
		// Expired: remove and try the exclusive create once more. If another
		// worker wins the race the next attempt reports held.
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			return false, nil
		}
	}
	return false, nil
}

// Renew extends a lease owned by holder; an expired or foreign lease fails.
func (s *FileLeaseStore) Renew(key, holder string, ttl time.Duration) (bool, error) {
	path := s.path(key)
	payload, err := s.read(path)
	if err != nil {
		return false, nil
	}
	if payload.Holder != holder || payload.ExpiresAt <= float64(time.Now().UnixNano())/1e9 {
		return false, nil
	}
	if err := s.write(path, holder, ttl); err != nil {
		return false, err
	}
	return true, nil
}

// Release deletes the lease only when holder still owns it.
func (s *FileLeaseStore) Release(key, holder string) (bool, error) {
	path := s.path(key)
	payload, err := s.read(path)
	if err != nil {
		return false, nil
	}
	if payload.Holder != holder {
		return false, nil
	}
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// NewHolderToken mints a host/pid/random holder identity.
func NewHolderToken() string {
	host, _ := os.Hostname()
	var random [8]byte
	f, err := os.Open("/dev/urandom")
	if err == nil {
		_, _ = f.Read(random[:])
		_ = f.Close()
	}
	token := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(random[:])
	return fmt.Sprintf("%s:%d:%s", host, os.Getpid(), token)
}

// WorkspaceLease serializes execs against one workspace handle. Acquire fails
// fast (the Python semantic: no queueing; the execution goes to an error
// terminal and an operator re-dispatches). The heartbeat renews for the whole
// exec; losing the lease does not interrupt the turn because the sandbox's own
// wall clock remains the backstop.
type WorkspaceLease struct {
	Store  LeaseStore
	Key    string
	TTL    time.Duration
	Holder string

	lost      atomic.Bool
	mu        sync.Mutex
	stop      chan struct{}
	done      chan struct{}
	once      sync.Once
	heartbeat bool
}

// NewWorkspaceLease builds a lease with a generated holder token.
func NewWorkspaceLease(store LeaseStore, key string, ttl time.Duration) *WorkspaceLease {
	return &WorkspaceLease{
		Store:  store,
		Key:    key,
		TTL:    ttl,
		Holder: NewHolderToken(),
	}
}

// Acquire takes the lease or returns ErrLeaseHeld.
func (l *WorkspaceLease) Acquire() error {
	ok, err := l.Store.Acquire(l.Key, l.Holder, l.TTL)
	if err != nil {
		return fmt.Errorf("sandbox lease acquire: %w", err)
	}
	if !ok {
		return fmt.Errorf("%w (key=%s): the same workspace cannot be executed concurrently; "+
			"if this is a leftover, inspect and re-dispatch/DLQ replay", ErrLeaseHeld, l.Key)
	}
	return nil
}

// StartHeartbeat renews at TTL/3 until Stop. Idempotent.
func (l *WorkspaceLease) StartHeartbeat() {
	l.mu.Lock()
	if l.heartbeat {
		l.mu.Unlock()
		return
	}
	l.heartbeat = true
	interval := l.TTL / 3
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	l.stop = make(chan struct{})
	l.done = make(chan struct{})
	l.mu.Unlock()

	go func() {
		defer close(l.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-l.stop:
				return
			case <-ticker.C:
				ok, err := l.Store.Renew(l.Key, l.Holder, l.TTL)
				if err != nil || !ok {
					l.lost.Store(true)
				}
			}
		}
	}()
}

// Lost reports whether a heartbeat renewal failed.
func (l *WorkspaceLease) Lost() bool { return l.lost.Load() }

// Release stops the heartbeat and drops the lease (best effort). Idempotent.
func (l *WorkspaceLease) Release() {
	l.once.Do(func() {
		l.mu.Lock()
		stop, done, started := l.stop, l.done, l.heartbeat
		l.heartbeat = false
		l.mu.Unlock()
		if started {
			close(stop)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
			}
		}
		_, _ = l.Store.Release(l.Key, l.Holder)
	})
}
