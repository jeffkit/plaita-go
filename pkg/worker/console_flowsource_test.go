package worker

// #299 §2 console read-through 负控/正控（先红后绿的「红」在基线为编译级：
// Config.ConsoleFlowSource* 与 pullThrough 均为本轮新符号，基线无接受面）。
//
// 四条任务书负控：
//  1. miss→拉取→成功             TestConsolePullThroughOnMiss
//  2. miss→404→具名错误(+负缓存)  TestConsolePullThrough404NegativeCached
//  3. console 不可达→瞬态降级     TestConsolePullThroughUnreachableTransient
//  4. 既有 FlowSource 命中零回归   TestRedisHitSkipsConsole
// 另钉：瞬态重试后成功 / latest 解析（draft 跳过）/ draft 版本拒载 /
// 租户头透传 / engineFlowPayload 注入口径。

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discardLogger() Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// serveVersionDetail 搭一个只实现版本详情端点的假 console。
func serveVersionDetail(t *testing.T, hits *atomic.Int32, status string, definition string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("X-Admin-API-Key") != "test-admin-key" {
			t.Errorf("missing X-Admin-API-Key header")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"flow_id":    "hello_world_go",
			"version":    "3",
			"status":     status,
			"runtime":    "python",
			"definition": definition,
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestConsolePullThroughOnMiss(t *testing.T) {
	ctx := context.Background()
	definition := string(mustHelloWorld(t))
	var hits atomic.Int32
	srv := serveVersionDetail(t, &hits, "published", definition)

	_, rdb, _, cfg := newTestWorker(t)
	cfg.ConsoleFlowSourceURL = srv.URL
	cfg.ConsoleFlowSourceAPIKey = "test-admin-key"
	cfg.ConsoleFlowSourceWriteBack = true
	fs := NewFlowSource(rdb, cfg, discardLogger())

	def, err := fs.GetFlowDefinition(ctx, "", "hello_world_go", "3")
	require.NoError(t, err)
	require.NotNil(t, def.Workflow)
	assert.Equal(t, "3", def.Version)
	assert.Equal(t, FlowHashAlgoV2, def.HashAlgo)

	// 指纹口径 = outbox 的 engine_payload：注入 flow_id/version 后对存储原文
	// 规范化求 sha256。回写键里的原文必须自洽于该指纹。
	stored, err := rdb.Get(ctx, cfg.KeyPrefix+":flow:hello_world_go:3").Bytes()
	require.NoError(t, err, "write-back must land the definition key")
	assert.Equal(t, def.Hash, mustHash(t, stored))
	var doc map[string]any
	require.NoError(t, json.Unmarshal(stored, &doc))
	assert.Equal(t, "hello_world_go", doc["flow_id"], "engine payload injects flow_id")
	assert.Equal(t, "3", doc["version"], "engine payload injects version")
	assert.Equal(t, "3", mustSMembers(t, rdb, cfg.KeyPrefix+":flow_versions:hello_world_go")[0])

	// 第二次调用走 TTL 缓存：console 命中数不增。
	_, err = fs.GetFlowDefinition(ctx, "", "hello_world_go", "3")
	require.NoError(t, err)
	assert.Equal(t, int32(1), hits.Load(), "second lookup must hit the local cache, not the console")
}

func TestConsolePullThrough404NegativeCached(t *testing.T) {
	ctx := context.Background()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	_, rdb, _, cfg := newTestWorker(t)
	cfg.ConsoleFlowSourceURL = srv.URL
	fs := NewFlowSource(rdb, cfg, discardLogger())

	_, err := fs.GetFlowDefinition(ctx, "", "ghost", "9")
	require.ErrorIs(t, err, ErrFlowNotFound, "console 404 = deterministic not-found")
	assert.NotErrorIs(t, err, ErrFlowLoad)

	// 负缓存窗口内的重试不打 console。
	_, err = fs.GetFlowDefinition(ctx, "", "ghost", "9")
	require.ErrorIs(t, err, ErrFlowNotFound)
	assert.Equal(t, int32(1), hits.Load(), "negative cache must absorb repeat misses")
}

func TestConsolePullThroughUnreachableTransient(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // 端口已关 = console 不可达

	_, rdb, _, cfg := newTestWorker(t)
	cfg.ConsoleFlowSourceURL = srv.URL
	fs := NewFlowSource(rdb, cfg, discardLogger())

	_, err := fs.GetFlowDefinition(ctx, "", "f", "1")
	require.ErrorIs(t, err, ErrFlowLoad, "unreachable console degrades to transient, not not-found")
	assert.NotErrorIs(t, err, ErrFlowNotFound,
		"不可达绝不能伪造成不存在——那会把在途执行终态化")
}

func TestConsolePullThroughRetriesTransientThenSucceeds(t *testing.T) {
	ctx := context.Background()
	definition := string(mustHelloWorld(t))
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		serveVersionDetailBody(w, definition)
	}))
	t.Cleanup(srv.Close)

	_, rdb, _, cfg := newTestWorker(t)
	cfg.ConsoleFlowSourceURL = srv.URL
	fs := NewFlowSource(rdb, cfg, discardLogger())

	def, err := fs.GetFlowDefinition(ctx, "", "hello_world_go", "3")
	require.NoError(t, err, "one transient 5xx must be retried, not surfaced")
	assert.Equal(t, "3", def.Version)
	assert.Equal(t, int32(2), calls.Load())
}

func TestRedisHitSkipsConsole(t *testing.T) {
	ctx := context.Background()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		t.Error("console must not be consulted when engine storage has the key")
	}))
	t.Cleanup(srv.Close)

	_, rdb, _, cfg := newTestWorker(t)
	cfg.ConsoleFlowSourceURL = srv.URL
	fs := NewFlowSource(rdb, cfg, discardLogger())
	require.NoError(t, fs.PresetFlowDefinition(ctx, "", "hello_world_go", "latest", mustHelloWorld(t)))

	def, err := fs.GetFlowDefinition(ctx, "", "hello_world_go", "")
	require.NoError(t, err)
	assert.Equal(t, "latest", def.Version)
	assert.Equal(t, int32(0), hits.Load(), "既有命中路径零回归：console 零调用")
}

func TestConsolePullThroughResolvesLatestPublished(t *testing.T) {
	ctx := context.Background()
	definition := string(mustHelloWorld(t))
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		require.Equal(t, "acme", r.Header.Get("X-Tenant-ID"), "任务租户必须透传为 X-Tenant-ID")
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/flows/hello_world_go":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"flow_id": "hello_world_go",
				"versions": []map[string]any{
					{"version": "1.0", "status": "published", "published_at": "2026-10-20T10:00:00"},
					{"version": "2.0", "status": "published", "published_at": "2026-10-21T10:00:00"},
					{"version": "3.0", "status": "draft"},
				},
			})
		case "/api/flows/hello_world_go/versions/2.0":
			serveVersionDetailBody(w, definition)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	_, rdb, _, cfg := newTestWorker(t)
	cfg.ConsoleFlowSourceURL = srv.URL
	fs := NewFlowSource(rdb, cfg, discardLogger())

	// 空 version + 引擎 Redis 无版本注册集 → resolveVersion 产 "latest" 哨兵 →
	// console 侧解析为最新 published（draft 跳过）。
	def, err := fs.GetFlowDefinition(ctx, "acme", "hello_world_go", "")
	require.NoError(t, err)
	assert.Equal(t, "2.0", def.Version)
	assert.Equal(t, int32(2), hits.Load(), "one flow-detail + one version-detail call")
}

func TestConsolePullThroughDraftVersionRejected(t *testing.T) {
	ctx := context.Background()
	var hits atomic.Int32
	srv := serveVersionDetail(t, &hits, "draft", string(mustHelloWorld(t)))

	_, rdb, _, cfg := newTestWorker(t)
	cfg.ConsoleFlowSourceURL = srv.URL
	cfg.ConsoleFlowSourceAPIKey = "test-admin-key"
	fs := NewFlowSource(rdb, cfg, discardLogger())

	_, err := fs.GetFlowDefinition(ctx, "", "hello_world_go", "3")
	require.ErrorIs(t, err, ErrFlowNotFound, "未发布版本不可执行（对齐契约端点 code=3）")
	assert.Contains(t, err.Error(), "not published")
}

func TestEngineFlowPayloadInjectsIdentity(t *testing.T) {
	raw, err := engineFlowPayload("fid", "7", `{"nodes":[],"inputType":1,"big":1234567890123456789}`)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))
	assert.Equal(t, "fid", doc["flow_id"])
	assert.Equal(t, "7", doc["version"])
	// 数字字面量经 json.Number 保真（不退化成 float64 再吐 1.2345678901234568e+18）。
	assert.Contains(t, string(raw), "1234567890123456789")
	// 指纹与等价手工文档一致（canonical 形式对同一文档稳定）。
	equivalent := fmt.Sprintf(`{"version":"7","nodes":[],"inputType":1,"big":1234567890123456789,"flow_id":"fid"}`)
	want, err := FlowDefinitionHash([]byte(equivalent))
	require.NoError(t, err)
	got, err := FlowDefinitionHash(raw)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func serveVersionDetailBody(w http.ResponseWriter, definition string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"flow_id":    "hello_world_go",
		"version":    "3",
		"status":     "published",
		"runtime":    "python",
		"definition": definition,
	})
}

func mustHash(t *testing.T, raw []byte) string {
	t.Helper()
	h, err := FlowDefinitionHash(raw)
	require.NoError(t, err)
	return h
}

func mustSMembers(t *testing.T, rdb *redis.Client, key string) []string {
	t.Helper()
	return rdb.SMembers(context.Background(), key).Val()
}
