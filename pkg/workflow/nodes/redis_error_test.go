package nodes

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// #73：redis 节点命令错误族（连接被拒 / 认证失败 / 命令级错误）必须具名上抛
// 并进入引擎重试分类，不得 `.Val()` 吞成「成功 + nil」。权威语义 = Python
// `execute_command` 出错即抛（plaita/node/redis.py:80-83）；`redis.Nil`（空
// bulk 回复）不是失败——Python 返回 None，Go 保持 Result=nil + err=nil。
//
// 负控口径：连接失败 / WRONGTYPE / NOAUTH 三例在修复前实测红（err==nil、
// Result==nil 的「伪成功」），修复后具名上抛；redis.Nil 边界用例修复前后都
// 必须绿（防修复过度把空回复当错误）。

func TestRedisNode_ConnectionFailureRetryable(t *testing.T) {
	// 真拨号路径（不注入 client）：端口 1 无人监听，连接必然被拒。
	node := &RedisNode{
		BaseNode:  BaseNode{ID: "redis", Next: "end"},
		Target:    "redis://127.0.0.1:1/0",
		Command:   "set",
		Arguments: "lock:order:42 holder",
	}
	res, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	if err == nil {
		t.Fatalf("connection refused must surface as an error (pre-fix: silent success with nil result)")
	}
	if !workflow.IsRetryableError(err) {
		t.Errorf("IsRetryableError(conn refused) = false, want true — dial/timeout family is transient; err = %v", err)
	}
	if res != nil {
		t.Errorf("ExecuteResult = %+v, want nil on error", res)
	}
}

func TestRedisNode_NilReplyNotAnError(t *testing.T) {
	// 真服务器路径：GET 不存在的 key → 空 bulk 回复 → go-redis 置 cmd.Err()
	// = redis.Nil。redismock 的空期望报的是「return value is required」自检
	// 错，不是 redis.Nil，所以这里不用 mock。
	mr := miniredis.RunT(t)
	node := &RedisNode{
		BaseNode:  BaseNode{ID: "redis", Next: "end"},
		Target:    "redis://" + mr.Addr(),
		Command:   "get",
		Arguments: "missing_key",
	}
	res, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	if err != nil {
		t.Fatalf("redis.Nil is not a failure (Python returns None): got %v", err)
	}
	if res == nil || res.Result != nil {
		t.Errorf("Result = %v, want nil (Python None parity)", res)
	}
}

func TestRedisNode_WrongTypeNamedNonRetryable(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.Set("k", "string-value") // string key，LPUSH 必然 WRONGTYPE

	node := &RedisNode{
		BaseNode:  BaseNode{ID: "redis-wrongtype", Next: "end"},
		Target:    "redis://" + mr.Addr(),
		Command:   "lpush",
		Arguments: "k x",
	}
	res, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	if err == nil {
		t.Fatalf("WRONGTYPE must surface as an error (pre-fix: silent success with nil result)")
	}
	if workflow.IsRetryableError(err) {
		t.Errorf("WRONGTYPE is deterministic (same args hit the same key type), got retryable; err = %v", err)
	}
	if !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Errorf("error text must carry the server-side marker, got: %v", err)
	}
	if !strings.Contains(err.Error(), "lpush") {
		t.Errorf("error text must name the command, got: %v", err)
	}
	if res != nil {
		t.Errorf("ExecuteResult = %+v, want nil on error", res)
	}
}

func TestRedisNode_AuthFailureNamedNonRetryable(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.RequireAuth("s3cret")

	// target 不带密码 → 服务端逐命令回 NOAUTH（密码轮换后的真实形态）。
	node := &RedisNode{
		BaseNode:  BaseNode{ID: "redis-auth", Next: "end"},
		Target:    "redis://" + mr.Addr(),
		Command:   "set",
		Arguments: "idempotency:order:42 done",
	}
	res, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	if err == nil {
		t.Fatalf("NOAUTH must surface as an error (pre-fix: silent success with nil result)")
	}
	if workflow.IsRetryableError(err) {
		t.Errorf("NOAUTH is deterministic (retry burns the budget without changing the verdict), got retryable; err = %v", err)
	}
	if !strings.Contains(err.Error(), "NOAUTH") {
		t.Errorf("error text must carry the server-side marker, got: %v", err)
	}
	// 脱敏红线：错误文本不得回显 target（URL userinfo 带密码）。
	if strings.Contains(err.Error(), mr.Addr()) {
		t.Errorf("error text must not echo the target, got: %v", err)
	}
	if res != nil {
		t.Errorf("ExecuteResult = %+v, want nil on error", res)
	}
}

func TestRedisNode_RequiredOutputAttribution(t *testing.T) {
	// output_type 声明 required 时，连接失败曾被 ValidateValue(nil) 抢先
	// 归因为「value is required but not provided」（#73 次生 bug）——错误
	// 检查必须在校验之前。
	node := &RedisNode{
		BaseNode: BaseNode{
			ID: "redis",
			OutputType: workflow.Property{
				DataType:   workflow.String,
				IsRequired: true,
			},
		},
		Target:    "redis://127.0.0.1:1/0",
		Command:   "get",
		Arguments: "k",
	}
	_, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	if err == nil {
		t.Fatalf("connection failure with required output must error")
	}
	if strings.Contains(err.Error(), "value is required but not provided") {
		t.Errorf("command failure must not be misattributed to a missing value, got: %v", err)
	}
	if !workflow.IsRetryableError(err) {
		t.Errorf("underneath the required-output scenario this is still a dial failure; err = %v", err)
	}
}
