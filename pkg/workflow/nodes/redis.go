package nodes

import (
	"context"
	"fmt"
	"github.com/go-ozzo/ozzo-validation"
	"github.com/go-redis/redis/v8"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/workflow"
	"net/url"
	"strconv"
	"strings"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeRedis,
		func(ctx context.Context) workflow.Node { return new(RedisNode) },
	)
}

// RedisNode represents a Redis node
type RedisNode struct {
	BaseNode

	Target    string `json:"target,omitempty"`
	Command   string `json:"command,omitempty"`
	Arguments string `json:"arguments,omitempty"`

	client *redis.Client
}

// Validate validates the Redis node
func (r *RedisNode) Validate() error {
	if r == nil {
		return nil
	}

	redisURL, err := url.Parse(r.Target)
	if err != nil {
		return fmt.Errorf("invalid target URL: %w", err)
	}

	username := redisURL.User.Username()
	password, _ := redisURL.User.Password()
	addr := redisURL.Host
	db := 0
	if len(redisURL.Path) > 1 {
		db, err = strconv.Atoi(redisURL.Path[1:])
		if err != nil {
			return fmt.Errorf("invalid DB number: %w", err)
		}
	}

	r.client = redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: username,
		Password: password,
		DB:       db,
	})

	return validation.ValidateStruct(r,
		validation.Field(&r.BaseNode, validation.Required),
		validation.Field(&r.Target, validation.Required, validation.Length(1, 1000)),
		validation.Field(&r.Command, validation.Required, validation.Length(1, 1000)),
		validation.Field(&r.client, validation.NotNil))
}

// GetType returns Redis node type
func (r *RedisNode) GetType() types.NodeType {
	return types.NodeTypeRedis
}

// Execute execute workflow Redis node
func (r *RedisNode) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	argStrings := strings.Fields(r.Arguments)
	args := make([]interface{}, len(argStrings)+1)
	args[0] = r.Command
	for i, arg := range argStrings {
		args[i+1] = arg
	}
	cmd := r.client.Do(ctx, args...)
	response := cmd.Val()

	if err := r.OutputType.ValidateValue(response); err != nil {
		return nil, fmt.Errorf("Redis node: output type validation: %w", err)
	}

	return &workflow.ExecuteResult{Result: response, NextNode: r.NextNode()}, nil
}
