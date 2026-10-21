package nodes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
	"github.com/spf13/cast"
)

// nodeTypeReport mirrors plaita-nodes ReportNode.node_type ("report").
const nodeTypeReport types.NodeType = "report"

func init() {
	workflow.RegisterNodeType(
		nodeTypeReport,
		func(ctx context.Context) workflow.Node { return new(Report) },
	)
}

const (
	reportFileMode = 0o644
	reportDirMode  = 0o755
	reportTokenDef = "default"
)

// reportNameRe is the single filename-component whitelist (plaita#201), the
// same pattern as the rate_limit key: ^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$.
var reportNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$`)

// Report is the run-level result channel atom: a generic jsonl append/read
// at <base_dir>/<token>.jsonl. Mirrors plaita-nodes ReportNode. It exists to
// route structured results around kernel limits (map-subflow end-expression
// recursion, if-block assignment scoping) — a workaround the Python side
// tracks for removal once the kernel fixes land (jeffkit/plaita#16).
//
// Fields: action ("write" default | "read"), entry (write object, supports
// expressions), base_dir (default <repo>/.flowcast/plaita-reports), repo
// (base for that default), token (run token; falls back to the global
// run_token, then "default").
//
// Outputs — write: {written, entry}; read: {entries}.
// The token is whitelist-validated before any filesystem touch (#201,
// fail-closed); a corrupted line is skipped on read (historical semantics);
// an unknown action is a named deterministic error, rejected before any FS
// action (A2R7-04).
type Report struct {
	BaseNode
	Action  string `json:"action,omitempty"`
	Entry   any    `json:"entry,omitempty"`
	Repo    any    `json:"repo,omitempty"`
	BaseDir any    `json:"base_dir,omitempty"`
	Token   any    `json:"token,omitempty"`
}

// GetType returns the report node type.
func (r *Report) GetType() workflow.NodeType { return nodeTypeReport }

// Execute validates the action name first, then resolves the report path and
// reads or appends.
func (r *Report) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	// Tenant gate (#36): base_dir/token are flow-controlled host faces (the
	// read action reads other tenants' channels just as freely as write
	// appends to them) — refused before any filesystem touch.
	if err := refuseNonDefaultTenantWriteFace(ctx, "report",
		"the node would read and write the host report channel"); err != nil {
		return nil, err
	}
	action := r.Action
	if action == "" {
		action = "write"
	}
	if action != "write" && action != "read" {
		return nil, fmt.Errorf("未知 report action: %q（write|read）", r.Action)
	}
	baseDir, err := r.evalBaseDir(ctx)
	if err != nil {
		return nil, err
	}
	token, err := r.evalToken(ctx)
	if err != nil {
		return nil, err
	}

	if action == "read" {
		entries, err := reportReadEntries(baseDir, token)
		if err != nil {
			return nil, fmt.Errorf("report %q: read entries: %w", r.ID, err)
		}
		return &workflow.ExecuteResult{Result: map[string]any{
			"entries": entries,
		}, NextNode: r.NextNode()}, nil
	}

	var entry any = map[string]any{}
	if r.Entry != nil {
		v, err := expression.Eval(ctx, r.Entry)
		if err != nil {
			return nil, fmt.Errorf("report %q: eval entry: %w", r.ID, err)
		}
		entry = v
	}
	if _, ok := entry.(map[string]any); !ok {
		entry = map[string]any{"value": entry}
	}
	if err := reportAppendEntry(baseDir, token, entry); err != nil {
		return nil, fmt.Errorf("report %q: append entry: %w", r.ID, err)
	}
	return &workflow.ExecuteResult{Result: map[string]any{
		"written": true,
		"entry":   entry,
	}, NextNode: r.NextNode()}, nil
}

func (r *Report) evalBaseDir(ctx context.Context) (string, error) {
	if r.BaseDir != nil {
		v, err := expression.Eval(ctx, r.BaseDir)
		if err != nil {
			return "", fmt.Errorf("report %q: eval base_dir: %w", r.ID, err)
		}
		if isTruthy(v) {
			return cast.ToString(v), nil
		}
	}
	repo := ""
	if r.Repo != nil {
		v, err := expression.Eval(ctx, r.Repo)
		if err != nil {
			return "", fmt.Errorf("report %q: eval repo: %w", r.ID, err)
		}
		if isTruthy(v) {
			repo = cast.ToString(v)
		}
	}
	if repo == "" {
		repo = "."
	}
	return filepath.Join(repo, ".flowcast", "plaita-reports"), nil
}

// evalToken resolves the run token: the evaluated token field wins, then the
// global run_token, then "default". Validation happens before any read/write
// touches the filesystem (#201).
func (r *Report) evalToken(ctx context.Context) (string, error) {
	raw := ""
	if r.Token != nil {
		v, err := expression.Eval(ctx, r.Token)
		if err != nil {
			return "", fmt.Errorf("report %q: eval token: %w", r.ID, err)
		}
		if isTruthy(v) {
			raw = cast.ToString(v)
		}
	}
	if raw == "" {
		if rc := runtime.WorkflowRuntimeContext(ctx); rc != nil {
			if g := rc.GetGlobalVariable("run_token"); isTruthy(g) {
				raw = cast.ToString(g)
			}
		}
	}
	if raw == "" {
		raw = reportTokenDef
	}
	return reportSafeComponent(raw, "report token")
}

// reportReadEntries reads the jsonl channel for (baseDir, token): blank
// lines are skipped, lines that fail to parse are skipped (historical
// semantics), a missing file yields an empty list. Other read errors
// propagate (Python's read_text raising).
func reportReadEntries(baseDir, token string) ([]any, error) {
	path, err := reportPath(baseDir, token)
	if err != nil {
		return nil, err
	}
	entries := []any{}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return entries, nil
		}
		return nil, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var item any
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			continue
		}
		entries = append(entries, item)
	}
	return entries, nil
}

// reportAppendEntry appends one JSON object per line to the channel for
// (baseDir, token). ensure_ascii=False is mirrored by raw UTF-8 output and
// disabled HTML escaping (Python's json.dumps leaves <,>,& untouched).
func reportAppendEntry(baseDir, token string, entry any) error {
	path, err := reportPath(baseDir, token)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), reportDirMode); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, reportFileMode)
	if err != nil {
		return err
	}
	defer f.Close()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	// Encode appends the trailing newline, matching json.dumps(...) + "\n".
	if err := enc.Encode(entry); err != nil {
		return err
	}
	_, err = f.Write(buf.Bytes())
	return err
}

// reportPath resolves the report file path behind the token whitelist
// (plaita#201): base_dir/<token>.jsonl.
func reportPath(baseDir, token string) (string, error) {
	name, err := reportSafeComponent(token, "report token")
	if err != nil {
		return "", err
	}
	return reportResolveChild(baseDir, name, "report token", ".jsonl")
}

// reportSafeComponent ports _safe_path.safe_component: a valid component is
// returned verbatim; anything else is a named error that carries kind,
// length and a SHA-256 fingerprint prefix — never the raw token.
func reportSafeComponent(value any, kind string) (string, error) {
	raw := cast.ToString(value)
	if !reportNameRe.MatchString(raw) {
		return "", fmt.Errorf(
			"%s 非法（长度 %d，指纹 %s）：只允许单个文件名分量 [A-Za-z0-9][A-Za-z0-9._-]{0,199}；"+
				"拒绝 . / .. / 路径分隔符 / URL 编码 / Unicode 同形字符。fail-closed：不静默改名、不落盘。",
			kind, len([]rune(raw)), reportFingerprint(raw))
	}
	return raw, nil
}

// reportResolveChild ports _safe_path.resolve_child: base/name+suffix,
// verified after symlink resolution to still live inside base; unresolvable
// paths and escapes both fail closed with the whitelist error shape.
func reportResolveChild(base, name, kind, suffix string) (string, error) {
	unresolvable := fmt.Errorf(
		"%s 路径无法解析（指纹 %s）：拒绝在无法校验命名空间时落盘。", kind, reportFingerprint(name))
	baseAbs, err := filepath.Abs(base)
	if err != nil {
		return "", unresolvable
	}
	candidate := filepath.Join(baseAbs, name+suffix)
	baseRes, err := reportResolveSoft(baseAbs)
	if err != nil {
		return "", unresolvable
	}
	candRes, err := reportResolveSoft(candidate)
	if err != nil {
		return "", unresolvable
	}
	if candRes != baseRes && !strings.HasPrefix(candRes, baseRes+string(os.PathSeparator)) {
		return "", fmt.Errorf(
			"%s 越出存储目录（指纹 %s）：fail-closed：不落盘。", kind, reportFingerprint(name))
	}
	return candidate, nil
}

// reportResolveSoft approximates pathlib.resolve(strict=False): resolve
// symlinks along the longest existing prefix; a missing tail stays literal.
// Errors other than not-exist (symlink loops, permissions) fail closed.
func reportResolveSoft(path string) (string, error) {
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

func reportFingerprint(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])[:12]
}
