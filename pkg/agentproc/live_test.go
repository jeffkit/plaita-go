package agentproc

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestLiveAgentTurn runs one real turn against a production agent from
// ~/.plaita. Guarded by PLAITA_LIVE=1 (costs one LLM call; skipped by
// default so CI never spends tokens):
//
//	PLAITA_LIVE=1 PLAITA_LIVE_AGENT=glm53-flash go test ./pkg/agentproc -run TestLiveAgentTurn -v
func TestLiveAgentTurn(t *testing.T) {
	if os.Getenv("PLAITA_LIVE") != "1" {
		t.Skip("set PLAITA_LIVE=1 (and optionally PLAITA_LIVE_AGENT) to run a real turn")
	}
	agent := os.Getenv("PLAITA_LIVE_AGENT")
	if agent == "" {
		agent = "glm53-flash"
	}
	spec, err := NewRegistry().ResolveAgent(agent)
	if err != nil {
		t.Fatalf("ResolveAgent(%q): %v", agent, err)
	}
	res, err := Run(context.Background(), spec, RunOptions{
		Message:   "Reply with exactly: ok",
		Timeout:   5 * time.Minute,
		Streaming: true,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("text=%q session=%q usage=%v", res.Text, res.SessionID, res.Usage)
	if strings.TrimSpace(res.Text) == "" {
		t.Fatal("empty reply body")
	}
}
