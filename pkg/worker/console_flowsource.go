package worker

// Console flow source（#299 §2 worker 读穿透）。
//
// 跨 Redis 部署（console 与 worker 不同 Redis）存在最终一致窗口：console 的
// durable publish outbox（plaita 2f1be94）把定义投递到目标 Redis 之前，
// engine storage 里没有 `{ns}:flow:{fid}:{ver}` 键，worker 只能
// ErrFlowNotFound。本文件让 FlowSource 在 Redis miss 时回源 console 管理面：
//
//	GET /api/flows/{flow_id}/versions/{version}   → VersionView（含 definition 原文与 status）
//	GET /api/flows/{flow_id}                      → FlowDetailResponse（versions 清单，空 version 解析 latest 用）
//
// 鉴权走管理面 API-key（X-Admin-API-Key = PLAITA_CONSOLE_ADMIN_API_KEY），
// 租户用 X-Tenant-ID 头声明（API-key 是平台身份，console 按头选租户并校验
// 租户存在性）——契约端点 /api/flowVersion/semver/detail 的租户归属由 HMAC
// 密钥决定、没有租户头，不适合 worker 按消息租户回源。
//
// 错误语义（对齐 Python #304 与本仓 errors.go 分类）：
//   - console 404 / 无 published 版本 / 版本未发布 → ErrFlowNotFound（确定性：
//     终态错误，进负缓存，不重投——console 也没有，重投不可能成功）；
//   - 网络错误 / 超时 / 5xx / 其余 4xx → ErrFlowLoad（瞬态：不 ack 等重投，
//     经 MaxDeliveries→DLQ 收敛）——console 不可达时消费循环照常运转，
//     绝不把「不可达」伪造成「不存在」，也不炸消费循环；
//   - 拉到的定义 unparsable / ir.Load 拒载 → ErrFlowNotFound（同 Redis 直读
//     路径的 not-loadable 口径：该引擎构建下永久失败，不烧重试预算）。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	// errConsoleNotFound: deterministic console-side miss (HTTP 404 / no
	// published version). Surfaced as ErrFlowNotFound and negative-cached.
	errConsoleNotFound = errors.New("console: not found")
	// errConsoleUnavailable: transient console failure (network, timeout,
	// 5xx, other 4xx). Surfaced as ErrFlowLoad — retried here up to
	// Config.ConsoleFlowSourceAttempts, then left to the carrier's own
	// redelivery path. Never negative-cached: the definition may appear any
	// moment (the outbox delivery window this feature exists for).
	errConsoleUnavailable = errors.New("console: unavailable")
)

// consoleMaxBodyBytes caps one console response read. Flow definitions are
// large but bounded (server golden fixtures are ~100KB); 64MB is far past any
// real definition and stops a broken console from OOMing the worker.
const consoleMaxBodyBytes = 64 << 20

// consoleRetryBackoff is the pause between read-through attempts. Fixed and
// small: the carrier-level retry (transient ErrFlowLoad → redelivery) is the
// real backpressure; this just rides out a connection blip.
const consoleRetryBackoff = 250 * time.Millisecond

type consoleFlowSource struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
	attempts   int
}

func newConsoleFlowSource(cfg *Config) *consoleFlowSource {
	return &consoleFlowSource{
		baseURL:    strings.TrimRight(cfg.ConsoleFlowSourceURL, "/"),
		apiKey:     cfg.ConsoleFlowSourceAPIKey,
		httpClient: &http.Client{Timeout: cfg.ConsoleFlowSourceTimeout},
		attempts:   cfg.ConsoleFlowSourceAttempts,
	}
}

// consoleVersion is the VersionView slice the worker consumes (definition 原文
// = flow_versions.definition 的存储文本，未注入 flow_id/version——注入在
// engineFlowPayload 按 outbox 的 engine_payload 口径做).
type consoleVersion struct {
	FlowID     string `json:"flow_id"`
	Version    string `json:"version"`
	Status     string `json:"status"`
	Definition string `json:"definition"`
}

type consoleVersionSummary struct {
	Version     string `json:"version"`
	Status      string `json:"status"`
	PublishedAt string `json:"published_at"`
	CreatedAt   string `json:"created_at"`
}

// versionDetail fetches GET /api/flows/{flow_id}/versions/{version}.
func (c *consoleFlowSource) versionDetail(ctx context.Context, tenant, flowID, version string) (*consoleVersion, error) {
	var v consoleVersion
	path := "/api/flows/" + url.PathEscape(flowID) + "/versions/" + url.PathEscape(version)
	if err := c.get(ctx, tenant, path, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// latestPublished resolves the sentinel "latest" against the console version
// list. Engine storage tracks a literal "latest" member, but console versions
// are concrete — the worker-side rule becomes「最新 published 版本」(newest by
// published_at, falling back to created_at, tie-broken by version string).
func (c *consoleFlowSource) latestPublished(ctx context.Context, tenant, flowID string) (string, error) {
	var detail struct {
		Versions []consoleVersionSummary `json:"versions"`
	}
	path := "/api/flows/" + url.PathEscape(flowID)
	if err := c.get(ctx, tenant, path, &detail); err != nil {
		return "", err
	}
	best, bestKey := "", ""
	for _, v := range detail.Versions {
		if v.Status != "published" {
			continue
		}
		key := v.PublishedAt
		if key == "" {
			key = v.CreatedAt
		}
		key += "\x00" + v.Version
		if best == "" || key > bestKey {
			best, bestKey = v.Version, key
		}
	}
	if best == "" {
		return "", errConsoleNotFound
	}
	return best, nil
}

// get performs one GET with retry on transient failures. 404 and non-transient
// results return immediately.
func (c *consoleFlowSource) get(ctx context.Context, tenant, path string, out any) error {
	var lastErr error
	for attempt := 0; attempt < c.attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(consoleRetryBackoff):
			}
		}
		err := c.getOnce(ctx, tenant, path, out)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errConsoleUnavailable) {
			return err
		}
		lastErr = err
	}
	return lastErr
}

func (c *consoleFlowSource) getOnce(ctx context.Context, tenant, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("%w: build request: %v", errConsoleUnavailable, err)
	}
	req.Header.Set("Accept", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-Admin-API-Key", c.apiKey)
	}
	if tenant != "" {
		req.Header.Set("X-Tenant-ID", tenant)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", errConsoleUnavailable, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return errConsoleNotFound
	case resp.StatusCode >= 400:
		return fmt.Errorf("%w: HTTP %d", errConsoleUnavailable, resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, consoleMaxBodyBytes)).Decode(out); err != nil {
		return fmt.Errorf("%w: decode response: %v", errConsoleUnavailable, err)
	}
	return nil
}

// engineFlowPayload mirrors plaita-console engine_outbox.engine_payload: the
// engine storage artifact is the console definition with flow_id/version
// injected (sync_flow_to_engine 和 outbox 投递落盘前做同样注入；outbox 登记
// 的 definition_hash 也按注入后载荷计)。Number literals survive via
// json.Number; the emitted bytes are the canonical form, whose sha256 equals
// Python's over the same document regardless of key order (#60 口径).
func engineFlowPayload(flowID, version, definition string) ([]byte, error) {
	dec := json.NewDecoder(strings.NewReader(definition))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	doc["flow_id"] = flowID
	doc["version"] = version
	injected, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return canonicalFlowDefinitionJSON(injected)
}
