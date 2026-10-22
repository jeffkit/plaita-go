package nodes

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// SQLResultLimitError mirrors plaita-nodes SqlResultLimitError
// (database.py:47-75, plaita#205): the SELECT result exceeded max_rows or
// max_bytes — fail-closed, no partial result is returned (silent truncation
// makes downstream compute on wrong data). Deterministic like the Python
// DeterministicError: the same SQL with the same limits fails again, so the
// engine must not burn retryTimes re-running the query.
type SQLResultLimitError struct {
	MaxRows       int
	MaxBytes      int
	ObservedRows  int
	ObservedBytes int
}

func (e *SQLResultLimitError) Error() string {
	return fmt.Sprintf(
		"sql_query result exceeds limits (max_rows=%d, max_bytes=%d): observed %d rows / %d bytes; "+
			"refusing to return partial results (no silent truncation, fail-closed). "+
			"Narrow the SQL with WHERE/LIMIT, or raise the node's max_rows/max_bytes.",
		e.MaxRows, e.MaxBytes, e.ObservedRows, e.ObservedBytes)
}

// sqlPositiveLimit validates a max_rows/max_bytes field: positive integers
// only, no silent clamping (database.py:117-131 `_positive_limit`, plaita#205
// 续: a 0/negative limit used to be quietly clamped to 1 — misconfiguration
// that looked like a normal run). Numeric strings coerce like Python's
// int(value); non-integral or non-numeric values are a named error.
func sqlPositiveLimit(name string, raw any, fallback int) (int, error) {
	if raw == nil {
		return fallback, nil
	}
	switch v := raw.(type) {
	case int:
		if v <= 0 {
			return 0, sqlPositiveLimitError(name, raw)
		}
		return v, nil
	case int64:
		if v <= 0 {
			return 0, sqlPositiveLimitError(name, raw)
		}
		return int(v), nil
	case float64:
		if v != float64(int64(v)) {
			return 0, fmt.Errorf("%s must be a positive integer (got %v)", name, v)
		}
		if v <= 0 {
			return 0, sqlPositiveLimitError(name, raw)
		}
		return int(v), nil
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return 0, fmt.Errorf("%s must be a positive integer (got %q)", name, v)
		}
		if n <= 0 {
			return 0, sqlPositiveLimitError(name, raw)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("%s must be a positive integer (got %T)", name, raw)
	}
}

func sqlPositiveLimitError(name string, got any) error {
	return fmt.Errorf(
		"%s must be a positive integer (got %v); non-positive values are rejected fail-closed", name, got)
}

// sqlIsWriteStatement classifies the statement by its leading keyword after
// whitespace/comments: explicit DML/DDL/transaction verbs run through
// ExecContext (rowcount = RowsAffected), everything else goes through
// QueryContext (rows path). Python asks the DBAPI `result.returns_rows` after
// execution (database.py:241); database/sql has no post-hoc probe, so the
// classification is lexical — writable CTEs (`WITH … INSERT`) still land on
// the rows path via the WITH default.
func sqlIsWriteStatement(sqlText string) bool {
	i := 0
	for i < len(sqlText) {
		c := sqlText[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '-' && i+1 < len(sqlText) && sqlText[i+1] == '-':
			if j := strings.IndexByte(sqlText[i:], '\n'); j < 0 {
				return false
			} else {
				i += j + 1
			}
		case c == '/' && i+1 < len(sqlText) && sqlText[i+1] == '*':
			j := strings.Index(sqlText[i+2:], "*/")
			if j < 0 {
				return false
			}
			i += j + 4
		case c == '(':
			i++
		default:
			rest := sqlText[i:]
			end := 0
			for end < len(rest) && (isSQLNameChar(rest[end])) {
				end++
			}
			switch strings.ToUpper(rest[:end]) {
			case "INSERT", "UPDATE", "DELETE", "REPLACE", "MERGE", "UPSERT",
				"CREATE", "ALTER", "DROP", "TRUNCATE", "COMMENT", "RENAME",
				"GRANT", "REVOKE", "VACUUM", "ANALYZE", "REINDEX", "CLUSTER",
				"CALL", "LOCK", "UNLOCK", "LOAD", "IMPORT", "COPY",
				"BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE",
				"SET", "RESET", "DISCARD", "LISTEN", "NOTIFY", "REFRESH":
				return true
			default:
				return false
			}
		}
	}
	return false
}

// sqlStatementError classifies a statement execution failure into the
// engine's retry taxonomy. Deliberate divergence from the Python engine
// default, documented in AGENTS.md: plaita classifies by exception shape and
// would retry every RuntimeError — including server-rejected SQL (syntax,
// missing table, bad credentials), where a retry re-executes the same failing
// statement. This node follows the redis #73 precedent instead:
//
//   - transport family (dial refused/reset, net errors, dead pool, ErrBadConn,
//     statement deadline) is retryable — a second attempt can genuinely succeed;
//   - context.Canceled is NOT retryable (it is cancellation, not failure);
//   - server-reply errors (pg SQLSTATE, mysql error numbers, anything
//     unrecognized) are deterministic — same SQL + same server state fails again.
//
// The message carries the node label; the DSN/credential URL is never echoed
// (userinfo red line), and driver messages pass through because the compiled
// drivers redact passwords in their error surfaces.
func sqlStatementError(label string, err error) error {
	if err == nil {
		return nil
	}
	wrapped := fmt.Errorf("%s: %w", label, err)
	if sqlStatementRetryable(err) {
		return workflow.RetryableError(wrapped)
	}
	return workflow.NonRetryable(wrapped)
}

// pgRetryableSQLSTATEs: connection exceptions (class 08), insufficient
// resources (53000/53100/53200/53300/53400), transaction-rollback retries
// (40001 serialization, 40002/40003, 40P01 deadlock), shutdown/cannot-connect
// (57P01/57P02/57P03). 57014 query_canceled is deliberately absent: that is
// cancellation, not a retryable failure.
var pgRetryableSQLSTATEs = map[string]bool{
	"08000": true, "08003": true, "08006": true, "08001": true, "08004": true, "08007": true,
	"08P01": true,
	"53000": true, "53100": true, "53200": true, "53300": true, "53400": true,
	"40001": true, "40002": true, "40003": true, "40P01": true,
	"57P01": true, "57P02": true, "57P03": true,
}

// mysqlRetryableErrnos: too many connections, lock wait timeout, deadlock,
// and the connection-lost family (2002/2003/2006/2013).
var mysqlRetryableErrnos = map[uint16]bool{
	1040: true, 1205: true, 1213: true,
	2002: true, 2003: true, 2006: true, 2013: true,
}

func sqlStatementRetryable(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, driver.ErrBadConn) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var sysErr *os.SyscallError
	if errors.As(err, &sysErr) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgRetryableSQLSTATEs[pgErr.Code]
	}
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		return mysqlRetryableErrnos[myErr.Number]
	}
	return false
}
