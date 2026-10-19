package sandbox

import (
	"errors"
	"os"
	"testing"
	"time"
)

func TestFileLeaseAcquireConflictRelease(t *testing.T) {
	store := &FileLeaseStore{Dir: t.TempDir()}
	ok, err := store.Acquire("k", "holder-a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("acquire = %v, %v", ok, err)
	}
	if ok, err := store.Acquire("k", "holder-b", time.Minute); err != nil || ok {
		t.Fatalf("second acquire = %v, %v (want held)", ok, err)
	}
	if ok, err := store.Release("k", "holder-b"); err != nil || ok {
		t.Fatalf("foreign release = %v, %v", ok, err)
	}
	if ok, err := store.Release("k", "holder-a"); err != nil || !ok {
		t.Fatalf("release = %v, %v", ok, err)
	}
	if ok, err := store.Acquire("k", "holder-b", time.Minute); err != nil || !ok {
		t.Fatalf("re-acquire = %v, %v", ok, err)
	}
}

func TestFileLeaseExpiredPreemption(t *testing.T) {
	store := &FileLeaseStore{Dir: t.TempDir()}
	if ok, err := store.Acquire("k", "first", 40*time.Millisecond); err != nil || !ok {
		t.Fatalf("acquire = %v, %v", ok, err)
	}
	time.Sleep(90 * time.Millisecond)
	if ok, err := store.Acquire("k", "second", time.Minute); err != nil || !ok {
		t.Fatalf("preempt = %v, %v", ok, err)
	}
	// The first holder must not be able to renew a lease it lost.
	if ok, err := store.Renew("k", "first", time.Minute); err != nil || ok {
		t.Fatalf("stale renew = %v, %v", ok, err)
	}
}

func TestFileLeaseRenewExtends(t *testing.T) {
	store := &FileLeaseStore{Dir: t.TempDir()}
	if ok, err := store.Acquire("k", "holder", 100*time.Millisecond); err != nil || !ok {
		t.Fatalf("acquire = %v, %v", ok, err)
	}
	time.Sleep(60 * time.Millisecond)
	if ok, err := store.Renew("k", "holder", 200*time.Millisecond); err != nil || !ok {
		t.Fatalf("renew = %v, %v", ok, err)
	}
	if ok, err := store.Acquire("k", "other", time.Minute); err != nil || ok {
		t.Fatalf("acquire after renew = %v, %v (want held)", ok, err)
	}
}

func TestWorkspaceLeaseFailsFastAndReleases(t *testing.T) {
	store := &FileLeaseStore{Dir: t.TempDir()}
	first := NewWorkspaceLease(store, "exec:ws", time.Minute)
	if err := first.Acquire(); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	second := NewWorkspaceLease(store, "exec:ws", time.Minute)
	if err := second.Acquire(); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("second acquire err = %v, want ErrLeaseHeld", err)
	}
	first.Release()
	if err := second.Acquire(); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	second.Release()
}

func TestWorkspaceLeaseHeartbeatDetectsLoss(t *testing.T) {
	store := &FileLeaseStore{Dir: t.TempDir()}
	lease := NewWorkspaceLease(store, "exec:ws", 150*time.Millisecond)
	if err := lease.Acquire(); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	lease.StartHeartbeat()
	defer lease.Release()

	// Remove the file behind the heartbeat; the next renew must fail.
	if err := os.Remove(store.path("exec:ws")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !lease.Lost() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !lease.Lost() {
		t.Fatal("heartbeat did not observe the lost lease")
	}
}
