package nodes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
	"github.com/spf13/cast"
)

// nodeTypeRateLimit mirrors plaita-nodes RateLimitNode.node_type ("rate_limit").
const nodeTypeRateLimit types.NodeType = "rate_limit"

func init() {
	workflow.RegisterNodeType(
		nodeTypeRateLimit,
		func(ctx context.Context) workflow.Node { return new(RateLimit) },
	)
}

// rateLimitStampCap bounds the stored timestamp list (enough for day/week
// windows); rateLimitDaySecs / rateLimitWeekSecs are the sliding windows.
const (
	rateLimitStampCap  = 100
	rateLimitDaySecs   = 86400.0
	rateLimitWeekSecs  = 7 * 86400.0
	rateLimitFileMode  = 0o644
	rateLimitDirMode   = 0o755
	rateLimitDailyDef  = 1
	rateLimitWeeklyDef = 5
)

// rateLimitNameRe is the single filename-component whitelist (plaita#201):
// ^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$ — the first char must be alphanumeric
// (rejects . / .. / .hidden), the rest allows only ASCII alphanumerics and
// ._- (no separators, no percent-escapes, no Unicode homoglyphs).
var rateLimitNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$`)

// RateLimit is the rate-limit atom backed by a per-key file counter
// (<store_dir>/<key>.json, a capped list of epoch timestamps). Mirrors
// plaita-nodes RateLimitNode:
//
//   - action: check | record | clear | acquire (acquire is the atomic
//     check+record under an exclusive flock — the deciding read and the
//     bookkeeping write happen in one critical section, so a crash or a
//     concurrent worker between the two steps cannot double-send);
//   - key: evaluated, must be a single filename component (whitelist,
//     fail-closed — UnsafePathName in Python);
//   - daily/weekly: limits used by check/acquire;
//   - store_dir: counter directory (default ~/.plaita-nodes/rate-limits).
//
// Outputs — check: {allowed, reason, today_count, week_count};
// acquire: {allowed, recorded, reason, today_count, week_count};
// record: {recorded, key, total}; clear: {cleared, key}.
// A corrupted counter file is treated as empty (historical semantics:
// a broken file must not deny service) but logged, not silent (#151).
type RateLimit struct {
	BaseNode
	Action   string           `json:"action,omitempty"`
	Key      any              `json:"key,omitempty"`
	Daily    *int             `json:"daily,omitempty"`
	Weekly   *int             `json:"weekly,omitempty"`
	StoreDir any              `json:"store_dir,omitempty"`
	Now      func() time.Time `json:"-"` // clock injection for tests
}

// GetType returns the rate_limit node type.
func (r *RateLimit) GetType() workflow.NodeType { return nodeTypeRateLimit }

func (r *RateLimit) dailyLimit() int {
	if r.Daily != nil {
		return *r.Daily
	}
	return rateLimitDailyDef
}

func (r *RateLimit) weeklyLimit() int {
	if r.Weekly != nil {
		return *r.Weekly
	}
	return rateLimitWeeklyDef
}

func (r *RateLimit) actionOrDefault() string {
	if r.Action == "" {
		return "check"
	}
	return r.Action
}

func (r *RateLimit) nowFloat() float64 {
	if r.Now != nil {
		return float64(r.Now().UnixNano()) / 1e9
	}
	return float64(time.Now().UnixNano()) / 1e9
}

// Execute evaluates key/store_dir, resolves the counter path behind the
// safety whitelist, then dispatches on action. Unknown actions are a named
// deterministic error (never retried).
func (r *RateLimit) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	// Tenant gate (#36): key/store_dir are flow-controlled — a tenant could
	// clear or exhaust another tenant's limiter on the shared host.
	if err := refuseNonDefaultTenantWriteFace(ctx, "rate_limit",
		"the node would read and write the host rate-limit counters"); err != nil {
		return nil, err
	}
	key, err := r.evalKey(ctx)
	if err != nil {
		return nil, err
	}
	dir, err := r.evalDir(ctx)
	if err != nil {
		return nil, err
	}
	// Second line of defense (#201): after resolution the path must still be
	// inside store_dir, otherwise fail closed.
	path, err := rateLimitResolveChild(dir, key, "rate_limit key", ".json")
	if err != nil {
		return nil, err
	}
	now := r.nowFloat()

	var out map[string]any
	switch r.actionOrDefault() {
	case "check":
		day, week := rateLimitCount(rateLimitLoad(path), now)
		allowed := day < r.dailyLimit() && week < r.weeklyLimit()
		out = map[string]any{
			"allowed":     allowed,
			"reason":      fmt.Sprintf("日 %d/%d，周 %d/%d", day, r.dailyLimit(), week, r.weeklyLimit()),
			"today_count": day,
			"week_count":  week,
		}
	case "acquire":
		out, err = r.acquire(path, now)
		if err != nil {
			return nil, err
		}
	case "record":
		stamps := append(rateLimitLoad(path), now)
		total := len(stamps)
		if err := rateLimitSave(path, rateLimitTail(stamps, rateLimitStampCap)); err != nil {
			return nil, fmt.Errorf("rate_limit %q: save counter: %w", r.ID, err)
		}
		out = map[string]any{"recorded": true, "key": key, "total": total}
	case "clear":
		if err := rateLimitSave(path, []float64{}); err != nil {
			return nil, fmt.Errorf("rate_limit %q: clear counter: %w", r.ID, err)
		}
		out = map[string]any{"cleared": true, "key": key}
	default:
		return nil, fmt.Errorf("未知 rate_limit action: %s（check|record|clear|acquire）", r.Action)
	}
	return &workflow.ExecuteResult{Result: out, NextNode: r.NextNode()}, nil
}

func (r *RateLimit) evalKey(ctx context.Context) (string, error) {
	raw := "default"
	if r.Key != nil {
		v, err := expression.Eval(ctx, r.Key)
		if err != nil {
			return "", fmt.Errorf("rate_limit %q: eval key: %w", r.ID, err)
		}
		if isTruthy(v) {
			raw = cast.ToString(v)
		}
	}
	// The key must be a single filename component (#201, whitelist
	// fail-closed): "../", absolute paths, "a/b", URL-escapes and Unicode
	// homoglyphs are all rejected — never silently renamed onto disk.
	return rateLimitSafeComponent(raw, "rate_limit key")
}

func (r *RateLimit) evalDir(ctx context.Context) (string, error) {
	if r.StoreDir != nil {
		v, err := expression.Eval(ctx, r.StoreDir)
		if err != nil {
			return "", fmt.Errorf("rate_limit %q: eval store_dir: %w", r.ID, err)
		}
		if isTruthy(v) {
			return cast.ToString(v), nil
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("rate_limit %q: resolve home dir: %w", r.ID, err)
	}
	return filepath.Join(home, ".plaita-nodes", "rate-limits"), nil
}

// acquire is the atomic check+record: inside an exclusive flock it decides
// and (when allowed) writes the slot back through the locked handle, so the
// mutual-exclusion window covers the whole read-modify-write.
func (r *RateLimit) acquire(path string, now float64) (map[string]any, error) {
	if err := os.MkdirAll(filepath.Dir(path), rateLimitDirMode); err != nil {
		return nil, fmt.Errorf("rate_limit %q: create store dir: %w", r.ID, err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, rateLimitFileMode)
	if err != nil {
		return nil, fmt.Errorf("rate_limit %q: open counter: %w", r.ID, err)
	}
	defer f.Close()
	locked := true
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		// Platforms without flock degrade to non-atomic, matching Python's
		// non-POSIX fallback (behavior = check+record as two steps).
		locked = false
	}
	if locked {
		defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	}

	stamps := rateLimitLoad(path)
	day, week := rateLimitCount(stamps, now)
	allowed := day < r.dailyLimit() && week < r.weeklyLimit()
	if allowed {
		stamps = rateLimitTail(append(stamps, now), rateLimitStampCap)
		data, err := json.Marshal(stamps)
		if err != nil {
			return nil, fmt.Errorf("rate_limit %q: marshal counter: %w", r.ID, err)
		}
		// Write back through the locked handle; the flush ordering of the
		// Python node is preserved by the unlock-before-close defers.
		if _, err := f.Seek(0, 0); err != nil {
			return nil, fmt.Errorf("rate_limit %q: rewind counter: %w", r.ID, err)
		}
		if err := f.Truncate(0); err != nil {
			return nil, fmt.Errorf("rate_limit %q: truncate counter: %w", r.ID, err)
		}
		if _, err := f.Write(data); err != nil {
			return nil, fmt.Errorf("rate_limit %q: write counter: %w", r.ID, err)
		}
	}
	dayN, weekN := day, week
	if allowed {
		dayN++
		weekN++
	}
	return map[string]any{
		"allowed":     allowed,
		"recorded":    allowed,
		"reason":      fmt.Sprintf("日 %d/%d，周 %d/%d", dayN, r.dailyLimit(), weekN, r.weeklyLimit()),
		"today_count": dayN,
		"week_count":  weekN,
	}, nil
}

// rateLimitLoad reads the counter file; a missing file is an empty counter,
// anything unreadable/unparsable is treated as empty with a warning (#151
// discipline: degrade, but never silently).
func rateLimitLoad(path string) []float64 {
	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Printf("rate_limit 计数器无法解析，按空计数继续：file=%s err=%v",
				filepath.Base(path), err)
		}
		return nil
	}
	var stamps []float64
	if err := json.Unmarshal(data, &stamps); err != nil {
		log.Printf("rate_limit 计数器无法解析，按空计数继续：file=%s err=%v",
			filepath.Base(path), err)
		return nil
	}
	return stamps
}

func rateLimitSave(path string, stamps []float64) error {
	if err := os.MkdirAll(filepath.Dir(path), rateLimitDirMode); err != nil {
		return err
	}
	data, err := json.Marshal(stamps)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, rateLimitFileMode)
}

// rateLimitCount mirrors the check filters: `now - t < window` (a future
// timestamp counts, like Python).
func rateLimitCount(stamps []float64, now float64) (day, week int) {
	for _, t := range stamps {
		if now-t < rateLimitDaySecs {
			day++
		}
		if now-t < rateLimitWeekSecs {
			week++
		}
	}
	return day, week
}

func rateLimitTail(stamps []float64, n int) []float64 {
	if len(stamps) > n {
		stamps = stamps[len(stamps)-n:]
	}
	if stamps == nil {
		stamps = []float64{}
	}
	return stamps
}

// rateLimitSafeComponent ports _safe_path.safe_component: a valid component
// is returned verbatim (no renaming/case-folding); anything else is a named
// error. The message never echoes the raw key (it may come from expressions
// or upstream data) — only kind, length and a SHA-256 fingerprint prefix.
func rateLimitSafeComponent(value any, kind string) (string, error) {
	raw := cast.ToString(value)
	if !rateLimitNameRe.MatchString(raw) {
		return "", fmt.Errorf(
			"%s 非法（长度 %d，指纹 %s）：只允许单个文件名分量 [A-Za-z0-9][A-Za-z0-9._-]{0,199}；"+
				"拒绝 . / .. / 路径分隔符 / URL 编码 / Unicode 同形字符。fail-closed：不静默改名、不落盘。",
			kind, len([]rune(raw)), rateLimitFingerprint(raw))
	}
	return raw, nil
}

// rateLimitResolveChild ports _safe_path.resolve_child: base/name+suffix,
// verified after symlink resolution to still live inside base. A path that
// cannot be safely resolved (symlink loops, permissions) or that escapes the
// store dir is a fail-closed error of the same shape as the whitelist error.
func rateLimitResolveChild(base, name, kind, suffix string) (string, error) {
	unresolvable := fmt.Errorf(
		"%s 路径无法解析（指纹 %s）：拒绝在无法校验命名空间时落盘。", kind, rateLimitFingerprint(name))
	baseAbs, err := filepath.Abs(base)
	if err != nil {
		return "", unresolvable
	}
	candidate := filepath.Join(baseAbs, name+suffix)
	baseRes, err := rateLimitResolveSoft(baseAbs)
	if err != nil {
		return "", unresolvable
	}
	candRes, err := rateLimitResolveSoft(candidate)
	if err != nil {
		return "", unresolvable
	}
	if candRes != baseRes && !strings.HasPrefix(candRes, baseRes+string(os.PathSeparator)) {
		return "", fmt.Errorf(
			"%s 越出存储目录（指纹 %s）：fail-closed：不落盘。", kind, rateLimitFingerprint(name))
	}
	return candidate, nil
}

// rateLimitResolveSoft approximates pathlib.resolve(strict=False): resolve
// symlinks along the longest existing prefix; a missing tail is kept literal.
// Errors other than not-exist (symlink loops, permissions) fail closed.
func rateLimitResolveSoft(path string) (string, error) {
	path = filepath.Clean(path)
	tail := ""
	cur := path
	for {
		resolved, err := filepath.EvalSymlinks(cur)
		if err == nil {
			return filepath.Join(resolved, tail), nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return path, nil
		}
		tail = filepath.Join(filepath.Base(cur), tail)
		cur = parent
	}
}

func rateLimitFingerprint(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])[:12]
}
