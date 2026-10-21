package nodes

// Shared requestTimeout budget across redirect hops (#35, plaita#267): the
// timeout of the WHOLE request no longer resets per hop. Both subtests red on
// a pre-fix baseline: there every hop restarted the full budget, so the two
// slow hops fit into it and the tiny-budget hop started "in time".

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
)

// TestHTTP_RequestTimeoutSharedAcrossRedirects: two 120ms hops cannot fit a
// 200ms whole-request budget — the second hop draws the REMAINING budget and
// fails inside it (per-hop reset would hand it a fresh 200ms and succeed).
func TestHTTP_RequestTimeoutSharedAcrossRedirects(t *testing.T) {
	var mu sync.Mutex
	hops := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/final", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("arrived"))
	})
	mux.HandleFunc("/hop", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hops++
		n := hops
		mu.Unlock()
		time.Sleep(120 * time.Millisecond)
		if n < 2 {
			http.Redirect(w, r, "/hop", http.StatusFound)
			return
		}
		http.Redirect(w, r, "/final", http.StatusFound)
	})
	s := httptest.NewServer(mux)
	defer s.Close()

	node := httpNodeFor(s.URL+"/hop", func(n *HTTP) {
		n.RequestTimeoutSecs = 0.2 // two 120ms hops cannot fit
		n.MaxRedirects = intPtr(5)
	})
	start := time.Now()
	_, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	code, msg := errCode(t, err)
	if code != httpDoRequestError {
		t.Fatalf("code = %d, want %d (1002; the budget exhaustion is a send failure, msg=%q)", code, httpDoRequestError, msg)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the shared budget let the request run %s — hops are resetting the budget", elapsed)
	}
	if !strings.Contains(msg, "context deadline exceeded") && !strings.Contains(msg, "requestTimeout budget exhausted") {
		t.Fatalf("message = %q, want a shared-budget failure", msg)
	}
}

// TestHTTP_RequestBudgetExhaustedNamedError: a budget already spent when a
// hop starts is rejected with the named exhaustion error before any dial
// (deterministic: a 1ns budget is always gone by the time the first hop is
// built). The request snapshot rides along → 1002.
func TestHTTP_RequestBudgetExhaustedNamedError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("no request may reach the peer once the budget is gone")
	}))
	defer s.Close()

	node := httpNodeFor(s.URL+"/hop", func(n *HTTP) {
		n.RequestTimeoutSecs = 1e-9 // 1ns: spent before the first hop starts
	})
	_, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	code, msg := errCode(t, err)
	if code != httpDoRequestError {
		t.Fatalf("code = %d, want %d", code, httpDoRequestError)
	}
	if !strings.Contains(msg, "requestTimeout budget exhausted") {
		t.Fatalf("message = %q, want the named requestTimeout budget exhaustion", msg)
	}
}
