package nodes

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	validation "github.com/go-ozzo/ozzo-validation"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		nodeTypeSQLQuery,
		func(ctx context.Context) workflow.Node { return new(SQLQuery) },
	)
}

// nodeTypeSQLQuery mirrors plaita-nodes SqlQueryNode.node_type ("sql_query",
// database.py:189; entry point pyproject.toml:52).
const nodeTypeSQLQuery types.NodeType = "sql_query"

// Defaults mirror plaita-nodes database.py:37-38 (plaita#205): SELECT
// results are capped at 1000 rows / 4 MiB (per-row JSON-encoded cumulative
// bytes) — exceeding either is a named fail-closed error, never a partial
// return.
const (
	sqlDefaultMaxRows  = 1000
	sqlDefaultMaxBytes = 4 * 1024 * 1024
)

// SQLQuery runs a SQL statement against the database a named credential
// points at. Mirrors plaita-nodes SqlQueryNode (database.py):
//
//   - SELECT returns {"rows": [...], "rowcount": len(rows)}; write statements
//     return {"rowcount": affected} (module docstring, database.py:9);
//   - max_rows/max_bytes default 1000 / 4 MiB, validated as positive integers
//     before any credential work, and exceeding either raises the named
//     SQLResultLimitError with check-limit-before-append semantics
//     (plaita#205: 不静默截断 — a partial result would make downstream
//     compute on wrong data). Python reads in adaptive fetchmany batches to
//     bound materialization; Go's rows.Next() streams row-at-a-time, so the
//     byte budget is checked before each row is kept and nothing beyond the
//     observed over-limit row is materialized;
//   - dry_run (node field or the flow's global dry_run) returns
//     {"rows": [], "rowcount": 0, "dry_run": true} without touching the
//     credential face or the database (database.py:200-202 — params may hold
//     sensitive values, dry-run does not look at them);
//   - query and every params value support $-expressions (database.py
//     `_resolve`: a string containing $ is evaluated, anything else passes
//     through; a falsy query renders empty and is rejected);
//   - `:name` bind parameters are rewritten to the driver's placeholder
//     style (see sql_params.go) — flows keep the sqlalchemy text() spelling.
//
// Runtime divergences, forced by the Go runtime (documented in AGENTS.md):
//   - cancellation rides the execution ctx (the A2R7-07 watchdog is
//     subsumed: QueryContext/ExecContext abort on ctx cancellation at the
//     driver level, and the node-level budget of the engine lands as a ctx
//     deadline exactly like the Python cancel_event);
//   - connection pools are cached per target with a bounded LRU (the
//     `_engine_cache`, database.py:33) — database/sql transparently re-dials
//     stale pooled connections, covering pool_pre_ping=True;
//   - error classification follows the redis #73 taxonomy instead of the
//     Python engine's retry-every-RuntimeError default (see
//     sqlStatementError) — transport failures are retryable, server-rejected
//     statements and limit breaches are not;
//   - non-default tenants are refused before the credential resolver runs:
//     the Go v1 credential face has no per-tenant stores (A42-06 posture).
type SQLQuery struct {
	BaseNode

	Credential string         `json:"credential,omitempty"` // credential NAME (resolved via the credential face)
	Query      any            `json:"query,omitempty"`      // SQL text or $-expression
	Params     map[string]any `json:"params,omitempty"`     // :name bind values (each value may be a $-expression)
	DryRun     bool           `json:"dry_run,omitempty"`
	MaxRows    any            `json:"max_rows,omitempty"` // positive int; pydantic-lax coercion of numeric strings
	MaxBytes   any            `json:"max_bytes,omitempty"`
}

// GetType returns the sql_query node type.
func (s *SQLQuery) GetType() types.NodeType { return nodeTypeSQLQuery }

// Validate checks the build-time contract: the literal limits must already
// be positive integers (pydantic enforces the int fields at model parse time
// — the Go counterpart is engine Validate, which runs before any execution;
// credential/query stay runtime-checked like Python's execute()-time
// ValueErrors).
func (s *SQLQuery) Validate() error {
	if err := validation.ValidateStruct(s,
		validation.Field(&s.BaseNode, validation.Required)); err != nil {
		return err
	}
	if _, err := sqlPositiveLimit("max_rows", s.MaxRows, sqlDefaultMaxRows); err != nil {
		return err
	}
	if _, err := sqlPositiveLimit("max_bytes", s.MaxBytes, sqlDefaultMaxBytes); err != nil {
		return err
	}
	return nil
}

// Execute mirrors database.py execute():201-283 order for order.
func (s *SQLQuery) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	// dry-run first: no credential parse, no dial (database.py:200-202 —
	// params may hold sensitive values, dry-run does not touch them).
	dry := s.DryRun
	if rc := runtime.WorkflowRuntimeContext(ctx); rc != nil {
		if gv := rc.GetGlobalVariable("dry_run"); isTruthy(gv) {
			dry = true
		}
	}
	if dry {
		return &workflow.ExecuteResult{Result: map[string]any{
			"rows":     []map[string]any{},
			"rowcount": 0,
			"dry_run":  true,
		}, NextNode: s.NextNode()}, nil
	}

	// Limit config errors surface before any credential work (database.py:203-205).
	rowLimit, err := sqlPositiveLimit("max_rows", s.MaxRows, sqlDefaultMaxRows)
	if err != nil {
		return nil, workflow.NonRetryable(err)
	}
	byteLimit, err := sqlPositiveLimit("max_bytes", s.MaxBytes, sqlDefaultMaxBytes)
	if err != nil {
		return nil, workflow.NonRetryable(err)
	}

	if strings.TrimSpace(s.Credential) == "" {
		return nil, workflow.NonRetryable(fmt.Errorf(
			"%s: credential field is required: fill in a credential name (created on the console credentials page)",
			sqlLabel(s.ID)))
	}

	// Tenant gate before the credential face runs (the v1 face is a
	// default-tenant store; see sql_credentials.go).
	if err := refuseNonDefaultTenantReasoned(ctx, "sql_query node",
		"the flow-controlled credential name would resolve against the default tenant's credential "+
			"store: the Go v1 credential face has no per-tenant stores, so a tenant-scoped database "+
			"must run on a worker of its own tenant (the code-node docker backend guidance in reverse)"); err != nil {
		return nil, err
	}

	payload, err := sqlResolveCredential(ctx, s.Credential)
	if err != nil {
		return nil, workflow.NonRetryable(fmt.Errorf("%s: %w", sqlLabel(s.ID), err))
	}
	target, err := sqlTargetFromCredential(payload)
	if err != nil {
		return nil, workflow.NonRetryable(fmt.Errorf("%s: %w", sqlLabel(s.ID), err))
	}

	sqlText, err := s.resolveQueryText(ctx)
	if err != nil {
		return nil, err
	}
	bind, err := s.resolveBindParams(ctx)
	if err != nil {
		return nil, err
	}

	// Execution-level cancel before any dial (database.py:217-220: refuse
	// before credentials/connection; ctx.Err() covers both cancel and the
	// engine's deadline).
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	pool, err := sqlPoolFor(target)
	if err != nil {
		return nil, workflow.NonRetryable(err)
	}
	return s.runStatement(ctx, pool, target, sqlText, bind, rowLimit, byteLimit)
}

// runStatement rewrites the bind parameters, classifies the statement, and
// takes the rows or the rowcount path.
func (s *SQLQuery) runStatement(ctx context.Context, pool *sql.DB, target sqlTarget, sqlText string, bind map[string]any, rowLimit, byteLimit int) (*workflow.ExecuteResult, error) {
	rewritten, args, err := rewriteNamedSQLParams(sqlText, bind, target.paramStyle)
	if err != nil {
		return nil, workflow.NonRetryable(fmt.Errorf("%s: %w", sqlLabel(s.ID), err))
	}

	if sqlIsWriteStatement(sqlText) {
		res, err := pool.ExecContext(ctx, rewritten, args...)
		if err != nil {
			return nil, sqlStatementError(sqlLabel(s.ID), err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, sqlStatementError(sqlLabel(s.ID), err)
		}
		return s.finish(map[string]any{"rowcount": int(affected)})
	}

	rows, err := pool.QueryContext(ctx, rewritten, args...)
	if err != nil {
		return nil, sqlStatementError(sqlLabel(s.ID), err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, sqlStatementError(sqlLabel(s.ID), err)
	}
	// A read-classified statement with no result set (e.g. a writable CTE
	// without RETURNING): Python would report the driver rowcount (often -1);
	// database/sql cannot produce one here, so 0 is reported (documented
	// divergence).
	if len(cols) == 0 {
		return s.finish(map[string]any{"rowcount": 0})
	}

	out := make([]map[string]any, 0)
	usedBytes := 0
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for rows.Next() {
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, sqlStatementError(sqlLabel(s.ID), err)
		}
		item := make(map[string]any, len(cols))
		for i, c := range cols {
			item[c] = sqlCellGoValue(vals[i])
		}
		encoded, err := sqlEncodedRow(item)
		if err != nil {
			return nil, workflow.NonRetryable(fmt.Errorf("%s: encode row: %w", sqlLabel(s.ID), err))
		}
		// Check the caps BEFORE the row joins the result (database.py:263-269):
		// the over-limit row itself is counted as observed, no partial result
		// is ever returned.
		size := len(encoded)
		if len(out)+1 > rowLimit || usedBytes+size > byteLimit {
			return nil, workflow.NonRetryable(&SQLResultLimitError{
				MaxRows:       rowLimit,
				MaxBytes:      byteLimit,
				ObservedRows:  len(out) + 1,
				ObservedBytes: usedBytes + size,
			})
		}
		out = append(out, item)
		usedBytes += size
	}
	if err := rows.Err(); err != nil {
		return nil, sqlStatementError(sqlLabel(s.ID), err)
	}
	return s.finish(map[string]any{"rows": out, "rowcount": len(out)})
}

func (s *SQLQuery) finish(result map[string]any) (*workflow.ExecuteResult, error) {
	if err := s.OutputType.ValidateValue(result); err != nil {
		return nil, fmt.Errorf("%s: output type validation: %w", sqlLabel(s.ID), err)
	}
	return &workflow.ExecuteResult{Result: result, NextNode: s.NextNode()}, nil
}

// resolveQueryText mirrors database.py:211: a string containing $ is
// evaluated, anything else passes through; falsy values render empty
// (Python's `str(x or "")`) and an empty statement is a named error.
func (s *SQLQuery) resolveQueryText(ctx context.Context) (string, error) {
	v := any(nil)
	if s.Query != nil {
		v = s.Query
	}
	if str, ok := v.(string); ok && strings.Contains(str, "$") {
		ev, err := expression.Eval(ctx, str)
		if err != nil {
			return "", fmt.Errorf("%s: eval query: %w", sqlLabel(s.ID), err)
		}
		v = ev
	}
	var text string
	switch t := v.(type) {
	case nil:
		text = ""
	case string:
		text = t
	default:
		if !isTruthy(t) {
			text = ""
		} else {
			text = fmt.Sprint(t)
		}
	}
	if strings.TrimSpace(text) == "" {
		return "", workflow.NonRetryable(fmt.Errorf("%s: query is empty", sqlLabel(s.ID)))
	}
	return strings.TrimSpace(text), nil
}

// resolveBindParams mirrors database.py:214-215: params resolve per value
// (evaluating the whole dict would turn literal dicts into None), nil stays
// nil (a NULL bind).
func (s *SQLQuery) resolveBindParams(ctx context.Context) (map[string]any, error) {
	out := make(map[string]any, len(s.Params))
	for k, v := range s.Params {
		if str, ok := v.(string); ok && strings.Contains(str, "$") {
			ev, err := expression.Eval(ctx, str)
			if err != nil {
				return nil, fmt.Errorf("%s: eval params[%q]: %w", sqlLabel(s.ID), k, err)
			}
			out[k] = ev
			continue
		}
		out[k] = v
	}
	return out, nil
}

// sqlEncodedRow renders a row the way database.py:261 does
// (json.dumps(..., ensure_ascii=False, default=str)): compact JSON with
// non-ASCII kept literal and HTML escaping off, so the byte budget counts
// UTF-8 bytes of the human-readable encoding. Go map keys sort differently
// from Python's insertion order, so the per-row byte count is budget
// accounting, not a cross-language byte-exact contract.
func sqlEncodedRow(item map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(item); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func sqlLabel(id types.NodeID) string {
	if id == "" {
		return "sql_query"
	}
	return fmt.Sprintf("sql_query %q", id)
}
