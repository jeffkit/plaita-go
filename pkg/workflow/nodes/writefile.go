package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeWritefile mirrors plaita-nodes WriteFileNode.node_type ("writefile").
const nodeTypeWritefile types.NodeType = "writefile"

const (
	// Same-name read side with plaita-nodes write_file.py / plaita
	// writefile_jail.py (cross-repo contract).
	writefileRootEnv       = "PLAITA_NODES_WORKSPACE_ROOT"
	writefileOptOutEnv     = "PLAITA_ALLOW_UNRESTRICTED_WRITES"
	writefileProjectRootEn = "PLAITA_PROJECT_ROOT"

	writefileDefaultIndent = 2
)

// writefileWorkspaceRootOverride mirrors WriteFileNode._WORKSPACE_ROOT_OVERRIDE
// (write_file.py:38): the process-level jail override deployers set at
// startup; it wins over the env var but not over the per-node field.
var writefileWorkspaceRootOverride string

// SetWritefileWorkspaceRootOverride installs the process-level jail root
// (deployment hook, mirroring the Python class-variable override).
func SetWritefileWorkspaceRootOverride(root string) {
	writefileWorkspaceRootOverride = root
}

// writefileDefaultRootCandidates supplies the default-jail derivation order
// (writefile_jail.py _default_root): PLAITA_PROJECT_ROOT → cwd → home.
// Variable (not a plain list) so tests can force the "nothing derivable"
// branch; "/" never constitutes a boundary and is skipped by the consumer.
var writefileDefaultRootCandidates = func() []string {
	var roots []string
	if p := os.Getenv(writefileProjectRootEn); p != "" {
		roots = append(roots, p)
	}
	if cwd, err := os.Getwd(); err == nil {
		roots = append(roots, cwd)
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		roots = append(roots, home)
	}
	return roots
}

// WriteFile writes one UTF-8 file, creating parent directories. Mirrors
// plaita-nodes WriteFileNode (write_file.py): json mode serializes the
// evaluated content (ensure_ascii=False parity: no HTML escaping), dry-run
// STILL writes (deliberate — draft dirs are temporary artifacts).
//
// Path jail (write_file.py:63-76 + writefile_jail.py): when a root is in
// effect (per-node workspace_root → process override → env PLAITA_NODES_
// WORKSPACE_ROOT), every target — relative or absolute — must resolve inside
// the root after symlink resolution; `..` traversal, absolute paths outside
// and symlinks pointing out are refused. Explicit opt-out
// PLAITA_ALLOW_UNRESTRICTED_WRITES=1 restores the historical unrestricted
// behavior for trusted single-machine deployments.
//
// Deliberate fail-closed divergences from Python (this engine's contract):
//   - when no root can be resolved at all, the node REFUSES to write
//     (Python falls back to unrestricted with a warning — the door-open
//     behavior writefile_jail.py exists to close);
//   - the default root derivation skips the "engine checkout promotion"
//     (plaita#51) — that rule is specific to the Python engine's checkout
//     layout, plaita-go has no plaita package tree to detect.
type WriteFile struct {
	BaseNode
	Path          any    `json:"path,omitempty"`    // expression
	Content       any    `json:"content,omitempty"` // expression
	JSONMode      bool   `json:"json"`              // pydantic Field(alias="json")
	Indent        *int   `json:"indent,omitempty"`  // default 2
	WorkspaceRoot string `json:"workspace_root,omitempty"`
}

func init() {
	workflow.RegisterNodeType(nodeTypeWritefile, func(ctx context.Context) workflow.Node { return new(WriteFile) })
}

// GetType returns the node type.
func (w *WriteFile) GetType() workflow.NodeType { return nodeTypeWritefile }

// Execute evaluates path/content and writes the file inside the jail.
func (w *WriteFile) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	if w.Path == nil {
		return nil, fmt.Errorf("writefile %q: missing path field", w.ID)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ev, err := expression.Eval(ctx, w.Path)
	if err != nil {
		return nil, fmt.Errorf("writefile %q: eval path: %w", w.ID, err)
	}
	raw := writefileStringify(ev)

	target := raw
	root, unrestricted, err := writefileResolveRoot(w.WorkspaceRoot)
	if err != nil {
		return nil, fmt.Errorf("writefile %q: %w", w.ID, err)
	}
	if !unrestricted {
		target, err = writefileJail(root, raw)
		if err != nil {
			return nil, fmt.Errorf("writefile %q: %w", w.ID, err)
		}
	}

	// Python: content = evaluate(self.content) if self.content is not None
	// else "" — a missing content field is the empty string, not null.
	var content any = ""
	if w.Content != nil {
		content, err = expression.Eval(ctx, w.Content)
		if err != nil {
			return nil, fmt.Errorf("writefile %q: eval content: %w", w.ID, err)
		}
	}
	var text string
	if w.JSONMode {
		indent := writefileDefaultIndent
		if w.Indent != nil {
			indent = *w.Indent
		}
		encoded, err := writefileMarshalJSON(content, indent)
		if err != nil {
			return nil, fmt.Errorf("writefile %q: %w", w.ID, err)
		}
		text = string(encoded)
	} else {
		text = writefileStringify(content)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return nil, fmt.Errorf("writefile %q: mkdir parents: %w", w.ID, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.WriteFile(target, []byte(text), 0o644); err != nil {
		return nil, fmt.Errorf("writefile %q: write: %w", w.ID, err)
	}
	return &workflow.ExecuteResult{Result: map[string]any{
		"path": target, "size": len(text),
	}, NextNode: w.NextNode()}, nil
}

// writefileResolveRoot mirrors the root resolution order: per-node
// workspace_root → process override → env ROOT_ENV → explicit opt-out
// (unrestricted, trusted single-machine only) → default derivation
// (writefile_jail._default_root). Fail-closed: nothing derivable → refuse.
func writefileResolveRoot(nodeRoot string) (root string, unrestricted bool, err error) {
	if nodeRoot != "" {
		return nodeRoot, false, nil
	}
	if writefileWorkspaceRootOverride != "" {
		return writefileWorkspaceRootOverride, false, nil
	}
	if v := os.Getenv(writefileRootEnv); v != "" {
		return v, false, nil
	}
	if os.Getenv(writefileOptOutEnv) == "1" {
		return "", true, nil
	}
	for _, cand := range writefileDefaultRootCandidates() {
		cand = strings.TrimSpace(cand)
		if cand == "" {
			continue
		}
		abs, aerr := filepath.Abs(cand)
		if aerr != nil {
			continue
		}
		if abs == string(os.PathSeparator) || abs == "" {
			continue // "/" 不构成边界（等于不设 jail）
		}
		return abs, false, nil
	}
	return "", false, errors.New(
		"无法推导 writefile 写入 jail（PLAITA_PROJECT_ROOT / 工作目录 / 家目录均不可用）：" +
			"refusing to write (fail-closed); set per-node workspace_root or " +
			writefileRootEnv)
}

// writefileJail confines raw inside root: the candidate (joined for relative
// paths, taken as-is for absolute ones — both converge into the jail,
// write_file.py:67) is fully symlink-resolved and must remain within the
// resolved root. Any escape — `..` traversal, absolute outside paths,
// symlinks (file or intermediate directory) pointing out — is refused.
func writefileJail(rootRaw, raw string) (string, error) {
	root, err := writefileResolveSymlinks(rawRootAbs(rootRaw))
	if err != nil {
		return "", fmt.Errorf("resolve workspace_root %q: %w (fail-closed)", rootRaw, err)
	}
	candidate := raw
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(root, candidate)
	}
	resolved, err := writefileResolveSymlinks(filepath.Clean(candidate))
	if err != nil {
		return "", fmt.Errorf("resolve path %q: %w (fail-closed)", raw, err)
	}
	sep := string(os.PathSeparator)
	if resolved != root && !strings.HasPrefix(resolved, root+sep) {
		return "", fmt.Errorf(
			"writefile path %q escapes workspace_root %q; refusing to write outside the jail "+
				"(set per-node workspace_root or env %s to configure the jail)", raw, root, writefileRootEnv)
	}
	return resolved, nil
}

func rawRootAbs(root string) string {
	abs, err := filepath.Abs(root)
	if err != nil {
		return filepath.Clean(root)
	}
	return filepath.Clean(abs)
}

// writefileResolveSymlinks resolves symlinks in the deepest existing ancestor
// of p and rejoins the non-existent tail (Python Path.resolve(strict=False)
// semantics). Fails closed when even the existing part cannot be resolved.
func writefileResolveSymlinks(p string) (string, error) {
	p = filepath.Clean(p)
	probe := p
	var tail []string
	for {
		if _, err := os.Lstat(probe); err == nil {
			break
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			break // reached the filesystem root
		}
		tail = append([]string{filepath.Base(probe)}, tail...)
		probe = parent
	}
	resolved, err := filepath.EvalSymlinks(probe)
	if err != nil {
		return "", err
	}
	for _, seg := range tail {
		resolved = filepath.Join(resolved, seg)
	}
	return filepath.Clean(resolved), nil
}

// writefileMarshalJSON mirrors json.dumps(content, ensure_ascii=False,
// indent=n) (write_file.py:79): non-ASCII stays literal, HTML characters are
// not escaped (Go's encoder would otherwise escape <,>,&).
func writefileMarshalJSON(v any, indent int) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if indent > 0 {
		enc.SetIndent("", strings.Repeat(" ", indent))
	}
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("serialize json content: %w", err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil // Encode appends \n; json.dumps doesn't
}

// writefileStringify mirrors Python str() for path/content positions
// (None renders as "None", Python parity — write_file.py:62/77/81).
func writefileStringify(v any) string {
	switch t := v.(type) {
	case nil:
		return "None"
	case string:
		return t
	case bool:
		if t {
			return "True"
		}
		return "False"
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case json.Number:
		return t.String()
	default:
		return fmt.Sprint(t)
	}
}
