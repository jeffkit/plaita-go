package nodes

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/go-redis/redis/v8"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeRedis,
		func(ctx context.Context) workflow.Node { return new(RedisNode) },
	)
}

// redisClientCacheMax mirrors plaita RedisNode.CLIENT_CACHE_MAX: clients are
// cached per evaluated target URL (target is an expression and may change per
// execution), a bounded LRU that closes evicted connections.
const redisClientCacheMax = 8

// redisTargetAllowlistEnv is the worker-level reachable-Redis declaration
// (#37): comma- or whitespace-separated redis URLs. Tenant-scoped flows may
// only point the redis node at explicitly declared instances.
const redisTargetAllowlistEnv = "PLAITA_REDIS_TARGET_ALLOWLIST"

// redisTargetAllowlistOverride is the process-level allowlist hook (same
// shape as SetWritefileWorkspaceRootOverride); it wins over the env var.
var redisTargetAllowlistOverride []string

// SetRedisTargetAllowlist installs the deployment's reachable-Redis allowlist
// (deployment hook): entries are redis URLs (userinfo/credentials never
// participate in matching) or bare host:port[/db]. The allowlist constrains
// only non-default tenants (#37); the default tenant keeps the historical
// semantics.
func SetRedisTargetAllowlist(targets []string) {
	redisTargetAllowlistOverride = targets
}

// redisTargetAllowlist resolves the effective allowlist: the process-level
// override wins, else the env var is parsed per call (read is cheap; tests
// can t.Setenv).
func redisTargetAllowlist() []string {
	if len(redisTargetAllowlistOverride) > 0 {
		return redisTargetAllowlistOverride
	}
	raw := os.Getenv(redisTargetAllowlistEnv)
	if raw == "" {
		return nil
	}
	var out []string
	for _, item := range strings.Fields(strings.ReplaceAll(raw, ",", " ")) {
		out = append(out, item)
	}
	return out
}

// redisTargetIdentity reduces a redis URL (or bare host:port[/db]) to the
// host:port/db identity the allowlist matches on: credentials in userinfo
// never participate, host comparison is case-insensitive. ok=false for
// anything without an explicit host and port (clientFor would reject it
// anyway).
func redisTargetIdentity(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw != "" && !strings.Contains(raw, "://") {
		raw = "redis://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.Port() == "" {
		return "", false
	}
	return strings.ToLower(u.Host) + u.Path, true
}

// redisTargetAllowlisted reports whether the evaluated target's identity is
// on the effective allowlist.
func redisTargetAllowlisted(target string) bool {
	id, ok := redisTargetIdentity(target)
	if !ok {
		return false
	}
	for _, entry := range redisTargetAllowlist() {
		if eid, entryOK := redisTargetIdentity(entry); entryOK && eid == id {
			return true
		}
	}
	return false
}

// RedisNode represents a Redis node
type RedisNode struct {
	BaseNode

	Target    string `json:"target,omitempty"`
	Command   string `json:"command,omitempty"`
	Arguments string `json:"arguments,omitempty"`

	// client is an explicitly injected client (DI/test hook, mirrors the
	// Python node's redis_client= injection): it takes priority over the
	// per-target cache and bypasses target resolution entirely.
	client *redis.Client

	mu          sync.Mutex
	clients     map[string]*redis.Client
	clientOrder []string
}

// Validate validates the Redis node. target/command/arguments all support
// plaita expressions (Python evaluates all three at execute time, see
// plaita.node.redis execute), so the target is deliberately NOT resolved
// into a client here — a flow whose target is `$INPUT.redisUrl` must not
// fail at load time with a URL-parse error.
func (r *RedisNode) Validate() error {
	if r == nil {
		return nil
	}

	return validation.ValidateStruct(r,
		validation.Field(&r.BaseNode, validation.Required),
		validation.Field(&r.Target, validation.Required, validation.Length(1, 1000)),
		validation.Field(&r.Command, validation.Required, validation.Length(1, 1000)))
}

// GetType returns Redis node type
func (r *RedisNode) GetType() types.NodeType {
	return types.NodeTypeRedis
}

// Execute evaluates target/command/arguments (all three support plaita
// expressions, matching Python `execution.evaluate` on each field), then
// splits the arguments on whitespace and runs the command.
func (r *RedisNode) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	evalField := func(raw, field string) (string, error) {
		ev, err := expression.Eval(ctx, raw)
		if err != nil {
			return "", fmt.Errorf("Redis node: eval %s: %w", field, err)
		}
		s, ok := ev.(string)
		if !ok {
			// plaita's evaluate result must be a string here (urlparse /
			// re.split would raise) — fail loudly instead of sending a
			// stringified expression result to Redis.
			return "", fmt.Errorf("Redis node: %s must evaluate to a string, got %T", field, ev)
		}
		return s, nil
	}

	// Tenant gate (#37): target/command/arguments are flow-controlled, so a
	// non-default tenant's flow can address the worker's own Redis — the
	// platform state store holding every tenant's execution state, queues,
	// leases and checkpoints. Fail-closed before any target resolution or
	// dial: a non-default tenant needs an explicitly configured target
	// allowlist, and its evaluated target must be on it. The injected
	// test client (r.client) bypasses target resolution, not this gate.
	restricted := runtime.IsNonDefaultTenant(runtime.TenantIDFromContext(ctx))
	if restricted && len(redisTargetAllowlist()) == 0 {
		return nil, refuseNonDefaultTenantReasoned(ctx, "redis node",
			"the flow-controlled target could reach the worker's own Redis — the platform "+
				"state store holding every tenant's execution state, queues, leases and "+
				"checkpoints; no redis target allowlist is configured for this worker "+
				"(set "+redisTargetAllowlistEnv+" or SetRedisTargetAllowlist), so every "+
				"redis target is refused for this tenant; the default tenant keeps the "+
				"historical semantics")
	}

	target, err := evalField(r.Target, "target")
	if err != nil {
		return nil, err
	}
	if restricted && !redisTargetAllowlisted(target) {
		return nil, refuseNonDefaultTenantReasoned(ctx, "redis node",
			"the evaluated target is not on the configured redis target allowlist "+
				"(matching is on host:port/db identity, credentials excluded); refusing to dial; "+
				"declare tenant-reachable instances via "+redisTargetAllowlistEnv+
				" or SetRedisTargetAllowlist; the default tenant keeps the historical semantics")
	}

	command, err := evalField(r.Command, "command")
	if err != nil {
		return nil, err
	}
	rawArgs, err := evalField(r.Arguments, "arguments")
	if err != nil {
		return nil, err
	}

	client, err := r.clientFor(target)
	if err != nil {
		return nil, err
	}

	// plaita: re.split(r"\s+", arguments.strip()) — a blank arguments string
	// yields a single empty argument (Python re.split quirk, kept for parity).
	trimmed := strings.TrimSpace(rawArgs)
	argList := []string{""}
	if trimmed != "" {
		argList = strings.Fields(trimmed)
	}

	args := make([]interface{}, 0, len(argList)+1)
	args = append(args, command)
	for _, arg := range argList {
		args = append(args, arg)
	}
	cmd := client.Do(ctx, args...)
	// #73: go-redis' Val() yields the zero value whenever Err() is set, so the
	// old `.Do(...).Val()` turned connection/auth/command failures into
	// "success with nil result" — no error event, no retry, no DLQ. Python's
	// execute_command raises on all of them (plaita/node/redis.py). redis.Nil
	// (empty bulk reply, e.g. GET on a missing key) is NOT a failure: Python
	// returns None there, so Go keeps Result=nil with err=nil.
	if err := cmd.Err(); err != nil && !errors.Is(err, redis.Nil) {
		// checked before OutputType.ValidateValue: a command failure must not
		// be misattributed as "value is required but not provided".
		return nil, r.commandError(command, err)
	}
	response := cmd.Val()

	if err := r.OutputType.ValidateValue(response); err != nil {
		return nil, fmt.Errorf("Redis node: output type validation: %w", err)
	}

	return &workflow.ExecuteResult{Result: response, NextNode: r.NextNode()}, nil
}

// commandError classifies a go-redis command error into the engine's retry
// taxonomy (#73): transport-level failures — dial refused/reset, timeouts,
// pool exhaustion, a client closed under LRU eviction — are retryable, a
// second attempt can genuinely succeed. Server reply errors (NOAUTH/WRONGPASS,
// WRONGTYPE/READONLY/OOM command not allowed, cluster MOVED/TRYAGAIN) and
// anything unrecognized are deterministic: the same arguments hit the same
// server state and fail again, so they must not burn the node's retry budget.
// The message carries the node ID and command name but never the target —
// redis URLs carry the password in userinfo (same redaction rule as
// clientFor).
func (r *RedisNode) commandError(command string, err error) error {
	label := "Redis node"
	if id := strings.TrimSpace(string(r.ID)); id != "" {
		label += " " + id
	}
	wrapped := fmt.Errorf("%s: command %q: %w", label, command, err)
	if redisTransientError(err) {
		return workflow.RetryableError(wrapped)
	}
	return workflow.NonRetryable(wrapped)
}

// redisTransientError reports whether err belongs to the transport family.
// The pool-timeout sentinel lives in go-redis v8's internal/pool (not
// re-exported), so it is matched by its stable text; everything else goes
// through errors.Is/errors.As on the exported surface.
func redisTransientError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, redis.ErrClosed) {
		return true
	}
	if strings.Contains(err.Error(), "redis: connection pool timeout") {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// clientFor returns the client for an evaluated target URL: an injected
// client wins; otherwise a bounded per-target LRU (plaita #81) — the same
// target reuses its client across rounds, evicted clients are closed.
func (r *RedisNode) clientFor(target string) (*redis.Client, error) {
	if r.client != nil {
		return r.client, nil
	}

	u, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("Redis node: invalid target URL: %w", err)
	}
	if u.Hostname() == "" || u.Port() == "" {
		// error message intentionally omits the target: redis URLs carry the
		// password in userinfo
		return nil, fmt.Errorf("Redis node: target must be redis://user:pass@host:port[/db] (host or port missing)")
	}
	password, _ := u.User.Password()
	db := 0
	if len(u.Path) > 1 {
		db, err = strconv.Atoi(u.Path[1:])
		if err != nil {
			return nil, fmt.Errorf("Redis node: invalid DB number: %w", err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.clients == nil {
		r.clients = map[string]*redis.Client{}
	}
	if cached, ok := r.clients[target]; ok {
		return cached, nil
	}
	// Transport guards mirror plaita RedisNode: 5s connect / 30s socket
	// timeouts — a hung socket must never pin the worker forever.
	client := redis.NewClient(&redis.Options{
		Addr:         u.Host,
		Username:     u.User.Username(),
		Password:     password,
		DB:           db,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	})
	r.clients[target] = client
	r.clientOrder = append(r.clientOrder, target)
	for len(r.clientOrder) > redisClientCacheMax {
		oldest := r.clientOrder[0]
		r.clientOrder = r.clientOrder[1:]
		if evicted, ok := r.clients[oldest]; ok {
			delete(r.clients, oldest)
			_ = evicted.Close()
		}
	}
	return client, nil
}
