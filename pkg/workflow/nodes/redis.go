package nodes

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/go-redis/redis/v8"

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

	target, err := evalField(r.Target, "target")
	if err != nil {
		return nil, err
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
	response := client.Do(ctx, args...).Val()

	if err := r.OutputType.ValidateValue(response); err != nil {
		return nil, fmt.Errorf("Redis node: output type validation: %w", err)
	}

	return &workflow.ExecuteResult{Result: response, NextNode: r.NextNode()}, nil
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
