package agentproc

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeBin writes a stand-in `recursive` binary: it records its argv and
// prints one --output-format json style result object.
func fakeBin(t *testing.T, resultJSON string) (bin string, argvFile string) {
	t.Helper()
	dir := t.TempDir()
	argvFile = filepath.Join(dir, "argv")
	outFile := filepath.Join(dir, "stdout.txt")
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" > " + argvFile + "\nprintf '%s\\n' '" + resultJSON + "' | tee " + outFile + "\n"
	bin = filepath.Join(dir, "recbin")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if data, err := os.ReadFile(outFile); err == nil {
			t.Logf("fake stdout: %s", data)
		}
	})
	return bin, argvFile
}

func recursiveDirectSpec(env map[string]string) *AgentSpec {
	if env == nil {
		env = map[string]string{}
	}
	return &AgentSpec{Name: "rec", Executor: ModeRecursiveDirect, Mode: ModeRecursiveDirect, Env: env, Model: "m1"}
}

func TestRecursiveDirectRunAndExtract(t *testing.T) {
	bin, argvFile := fakeBin(t, `{"is_error":false,"result":"ok reply","session_id":"rc-1","usage":{"input_tokens":5}}`)
	env := map[string]string{"RECURSIVE_BIN": bin, "RECURSIVE_WORKSPACE": "/ws", "RECURSIVE_MODEL": "m1"}

	res, err := Run(context.Background(), recursiveDirectSpec(env), RunOptions{Message: "do it", Timeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "ok reply" || res.SessionID != "rc-1" || res.Usage == nil {
		t.Errorf("res = %+v", res)
	}
	argv, _ := os.ReadFile(argvFile)
	joined := string(argv)
	for _, want := range []string{"--workspace /ws", "--output-format json", "--permission-mode auto", "--model m1", "run do it"} {
		if !strings.Contains(joined, want) {
			t.Errorf("argv missing %q: %s", want, joined)
		}
	}
}

func TestRecursiveDirectResumeArgv(t *testing.T) {
	bin, argvFile := fakeBin(t, `{"is_error":false,"result":"resumed","session_id":"rc-1"}`)
	env := map[string]string{"RECURSIVE_BIN": bin}
	_, err := Run(context.Background(), recursiveDirectSpec(env), RunOptions{Message: "continue", SessionID: "sess-7", Timeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	argv, _ := os.ReadFile(argvFile)
	if !strings.Contains(string(argv), "resume sess-7 --message continue") {
		t.Errorf("argv = %s", argv)
	}
}

func TestRecursiveDirectIsError(t *testing.T) {
	bin, _ := fakeBin(t, `{"is_error":true,"result":"upstream exploded","session_id":"rc-2"}`)
	env := map[string]string{"RECURSIVE_BIN": bin}
	res, err := Run(context.Background(), recursiveDirectSpec(env), RunOptions{Message: "m", Timeout: 30 * time.Second})
	if err == nil || !strings.Contains(err.Error(), "upstream exploded") {
		t.Fatalf("err = %v", err)
	}
	if res == nil || res.SessionID != "rc-2" {
		t.Errorf("session should still be learned: %+v", res)
	}
}

func TestRecursiveDirectRawFallback(t *testing.T) {
	bin, _ := fakeBin(t, `just plain words, no json`)
	env := map[string]string{"RECURSIVE_BIN": bin}
	res, err := Run(context.Background(), recursiveDirectSpec(env), RunOptions{Message: "m", Timeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "just plain words, no json" {
		t.Errorf("text = %q", res.Text)
	}
}

func TestExtractRecursiveResultBraceScan(t *testing.T) {
	// production shape: the result object is the last LINE-START `{`
	// (scanned last-first), nested braces inside strings are honoured
	stdout := "noise { not json\n{\"a\":1}\nfinal: {\"x\":1}\n{\"result\":\"r1\",\"is_error\":false,\"nested\":{\"x\":\"}\"}}\n"
	obj := extractRecursiveResult(stdout)
	if obj["result"] != "r1" {
		t.Errorf("obj = %v", obj)
	}
	// fallback: mid-line `{"` object when no line-start match decodes
	obj2 := extractRecursiveResult("log line {\nbroken\nq: {\"result\":\"r2\"}\n")
	if obj2["result"] != "r2" {
		t.Errorf("fallback obj = %v", obj2)
	}
}
