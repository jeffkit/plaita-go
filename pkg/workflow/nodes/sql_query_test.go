package nodes

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// 安装进程级凭据 resolver（用完恢复）；PLAITA_CREDENTIALS_FILE 文件钩子的
// 行为另有专测。非并行测试包纪律与 redisTargetAllowlistOverride 相同。
func sqlTestResolver(t *testing.T, store map[string]map[string]any) {
	t.Helper()
	prev := sqlCredentialResolverOverride
	sqlCredentialResolverOverride = func(_ context.Context, name string) (map[string]any, error) {
		payload, ok := store[name]
		if !ok {
			return nil, fmt.Errorf("credential %q not found", name)
		}
		return payload, nil
	}
	t.Cleanup(func() { sqlCredentialResolverOverride = prev })
}

func sqlTestSQLiteURL(name string) string {
	// opaque URL form: a "file:…" authority cannot be spelled //host (url.Parse
	// would read it as host:port) — this spelling sqlalchemy accepts too.
	return fmt.Sprintf("sqlite:file:%s?mode=memory&cache=shared", name)
}

// 打开同一条共享缓存内存库的准备句柄并建表；句柄活到测试结束，内存库不塌。
func sqlTestSQLiteSetup(t *testing.T, name string, ddl ...string) {
	t.Helper()
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=memory&cache=shared", name))
	if err != nil {
		t.Fatalf("open setup db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, stmt := range ddl {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("setup %q: %v", stmt, err)
		}
	}
}

func sqlQueryNode(id string, mutate func(*SQLQuery)) *SQLQuery {
	n := &SQLQuery{BaseNode: BaseNode{ID: workflow.NodeID(id)}}
	if mutate != nil {
		mutate(n)
	}
	return n
}

func runSQL(t *testing.T, n *SQLQuery, ctx context.Context) (map[string]any, error) {
	t.Helper()
	if ctx == nil {
		ctx = gateTestCtx(t, map[string]any{}, nil)
	}
	res, err := n.Execute(ctx)
	if err != nil {
		return nil, err
	}
	out, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type %T", res.Result)
	}
	return out, nil
}

// --- 基本查询 / 写操作形状（database.py:9 模块 docstring 形状对拍） ---

func TestSQLQueryBasicSelectShape(t *testing.T) {
	sqlTestResolver(t, map[string]map[string]any{
		"app-db": {"url": sqlTestSQLiteURL("sqlunit_basic")},
	})
	sqlTestSQLiteSetup(t, "sqlunit_basic",
		`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, name TEXT)`,
		`DELETE FROM t`,
		`INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob')`)

	out, err := runSQL(t, sqlQueryNode("q1", func(n *SQLQuery) {
		n.Credential = "app-db"
		n.Query = "SELECT id, name FROM t ORDER BY id"
	}), nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out["rowcount"] != 2 {
		t.Errorf("rowcount = %v, want 2 (len(rows), database.py:282)", out["rowcount"])
	}
	rows, ok := out["rows"].([]map[string]any)
	if !ok || len(rows) != 2 {
		t.Fatalf("rows = %T len mismatch", out["rows"])
	}
	if rows[0]["name"] != "alice" || rows[0]["id"] != int64(1) {
		t.Errorf("row0 = %v", rows[0])
	}
	if _, has := out["dry_run"]; has {
		t.Errorf("plain select must not carry dry_run key")
	}
}

func TestSQLQueryWriteStatementRowcount(t *testing.T) {
	sqlTestResolver(t, map[string]map[string]any{
		"app-db": {"url": sqlTestSQLiteURL("sqlunit_write")},
	})
	sqlTestSQLiteSetup(t, "sqlunit_write",
		`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, name TEXT)`,
		`DELETE FROM t`)

	out, err := runSQL(t, sqlQueryNode("w1", func(n *SQLQuery) {
		n.Credential = "app-db"
		n.Query = "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')"
	}), nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out["rowcount"] != 2 {
		t.Errorf("rowcount = %v, want 2 (affected rows)", out["rowcount"])
	}
	if _, has := out["rows"]; has {
		t.Errorf("write result must not carry rows (database.py:283)")
	}

	out, err = runSQL(t, sqlQueryNode("w2", func(n *SQLQuery) {
		n.Credential = "app-db"
		n.Query = "UPDATE t SET name = 'z' WHERE id <= 1"
	}), nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out["rowcount"] != 1 {
		t.Errorf("update rowcount = %v, want 1", out["rowcount"])
	}
}

// --- 负控面（具名上限错误 / dry-run / 配置错误 / 连接失败分类） ---

func TestSQLQueryRowLimitNamedFailClosed(t *testing.T) {
	sqlTestResolver(t, map[string]map[string]any{
		"app-db": {"url": sqlTestSQLiteURL("sqlunit_rowlimit")},
	})
	sqlTestSQLiteSetup(t, "sqlunit_rowlimit",
		`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)`,
		`DELETE FROM t`,
		`INSERT INTO t (id) VALUES (1), (2), (3), (4), (5)`)

	_, err := runSQL(t, sqlQueryNode("lim", func(n *SQLQuery) {
		n.Credential = "app-db"
		n.Query = "SELECT id FROM t"
		n.MaxRows = 2
	}), nil)
	if err == nil {
		t.Fatal("want named limit error")
	}
	var limErr *SQLResultLimitError
	if !errors.As(err, &limErr) {
		t.Fatalf("error %v is not *SQLResultLimitError", err)
	}
	if limErr.MaxRows != 2 || limErr.ObservedRows != 3 {
		t.Errorf("limit error fields = %+v, want max_rows=2 observed=3 (len(rows)+1, database.py:267)", limErr)
	}
	if workflow.IsRetryableError(err) {
		t.Errorf("limit breach is deterministic (SqlResultLimitError = DeterministicError): must not retry")
	}
}

func TestSQLQueryByteLimitNamedFailClosed(t *testing.T) {
	sqlTestResolver(t, map[string]map[string]any{
		"app-db": {"url": sqlTestSQLiteURL("sqlunit_bytelimit")},
	})
	sqlTestSQLiteSetup(t, "sqlunit_bytelimit",
		`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, note TEXT)`,
		`DELETE FROM t`,
		`INSERT INTO t (id, note) VALUES (1, 'abcdefgh'), (2, 'ijklmnop')`)

	_, err := runSQL(t, sqlQueryNode("blim", func(n *SQLQuery) {
		n.Credential = "app-db"
		n.Query = "SELECT id, note FROM t"
		n.MaxBytes = 32 // one row of `{"id":1,"note":"abcdefgh"}` fits, two don't
	}), nil)
	if err == nil {
		t.Fatal("want named byte-limit error")
	}
	var limErr *SQLResultLimitError
	if !errors.As(err, &limErr) {
		t.Fatalf("error %v is not *SQLResultLimitError", err)
	}
	if limErr.ObservedBytes <= limErr.MaxBytes {
		t.Errorf("observed bytes %d must exceed max_bytes %d", limErr.ObservedBytes, limErr.MaxBytes)
	}
	if workflow.IsRetryableError(err) {
		t.Errorf("byte-limit breach must be deterministic")
	}
}

func TestSQLQueryDryRunShortCircuit(t *testing.T) {
	// 凭据面完全未配置：dry-run 不得触碰 resolver/文件（database.py:200-202）。
	node := sqlQueryNode("d1", func(n *SQLQuery) { n.DryRun = true })
	out, err := runSQL(t, node, nil)
	if err != nil {
		t.Fatalf("dry-run must succeed without any credential face: %v", err)
	}
	if out["rowcount"] != 0 {
		t.Errorf("rowcount = %v, want 0", out["rowcount"])
	}
	if out["dry_run"] != true {
		t.Errorf("dry_run flag = %v", out["dry_run"])
	}
	rows, ok := out["rows"].([]map[string]any)
	if !ok || len(rows) != 0 {
		t.Errorf("rows must be an empty (non-nil) list, got %T", out["rows"])
	}

	// globalContext.dry_run 同样短路（capture 同款读法）。
	rc := &runtime.Context{
		WorkflowInput:      map[string]any{},
		ExecuteNodeResults: map[string]any{},
		EnvVariables:       map[string]any{},
		GlobalVariables:    map[string]any{},
	}
	rc.WithGlobalVariable("dry_run", true)
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), rc)
	out, err = runSQL(t, sqlQueryNode("d2", nil), ctx)
	if err != nil {
		t.Fatalf("global dry-run: %v", err)
	}
	if out["dry_run"] != true {
		t.Errorf("global dry_run short-circuit missing")
	}
}

func TestSQLQueryConfigErrorsFailClosed(t *testing.T) {
	sqlTestResolver(t, map[string]map[string]any{
		"app-db": {"url": sqlTestSQLiteURL("sqlunit_cfg")},
	})

	cases := []struct {
		name     string
		mutate   func(*SQLQuery)
		contains string
	}{
		{"missing credential", func(n *SQLQuery) { n.Query = "SELECT 1" }, "credential field is required"},
		{"empty query", func(n *SQLQuery) { n.Credential = "app-db"; n.Query = "   " }, "query is empty"},
		{"max_rows zero", func(n *SQLQuery) { n.Credential = "app-db"; n.Query = "SELECT 1"; n.MaxRows = 0 },
			"max_rows must be a positive integer (got 0)"},
		{"max_rows negative", func(n *SQLQuery) { n.Credential = "app-db"; n.Query = "SELECT 1"; n.MaxRows = -5 },
			"max_rows must be a positive integer (got -5)"},
		{"max_rows fractional", func(n *SQLQuery) { n.Credential = "app-db"; n.Query = "SELECT 1"; n.MaxRows = 2.5 },
			"max_rows must be a positive integer (got 2.5)"},
		{"max_rows junk string", func(n *SQLQuery) { n.Credential = "app-db"; n.Query = "SELECT 1"; n.MaxRows = "abc" },
			"max_rows must be a positive integer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := sqlQueryNode("cfg", tc.mutate)
			_, err := runSQL(t, n, nil)
			if err == nil {
				t.Fatalf("%s: want named error", tc.name)
			}
			if tc.contains != "" && !strings.Contains(err.Error(), tc.contains) {
				t.Errorf("error %q misses %q", err.Error(), tc.contains)
			}
			if workflow.IsRetryableError(err) {
				t.Errorf("%s: config errors are deterministic (ValueError parity), got retryable", tc.name)
			}
		})
	}
}

func TestSQLQueryUnknownSchemeNamed(t *testing.T) {
	sqlTestResolver(t, map[string]map[string]any{
		"app-db": {"url": "oracle://scott:tiger@host1:1521/xepdb1"},
	})
	_, err := runSQL(t, sqlQueryNode("ora", func(n *SQLQuery) {
		n.Credential = "app-db"
		n.Query = "SELECT 1"
	}), nil)
	if err == nil || !strings.Contains(err.Error(), `unsupported connection scheme "oracle"`) {
		t.Fatalf("want named scheme error, got %v", err)
	}
	if !strings.Contains(err.Error(), "postgres") || !strings.Contains(err.Error(), "sqlite") {
		t.Errorf("error should list supported families: %v", err)
	}
}

// max_rows/max_bytes 的 pydantic-lax 宽松面：整数字符串/整值浮点收（pydantic
// int lax 同款），负值在 Validate（引擎构建期，对应 pydantic 模型解析期）就拒。
func TestSQLQueryValidateLimits(t *testing.T) {
	n := sqlQueryNode("v1", func(n *SQLQuery) { n.MaxRows = "500"; n.MaxBytes = 2048.0 })
	if err := n.Validate(); err != nil {
		t.Fatalf("lax literal limits must validate: %v", err)
	}
	n = sqlQueryNode("v2", func(n *SQLQuery) { n.MaxRows = 0 })
	if err := n.Validate(); err == nil || !strings.Contains(err.Error(), "max_rows must be a positive integer") {
		t.Fatalf("Validate must reject non-positive max_rows, got %v", err)
	}
}

// --- 连接失败 → 可重试分类 + 脱敏红线（负控 4 的节点级细化） ---

func TestSQLQueryConnectionFailureRetryableAndRedacted(t *testing.T) {
	sqlTestResolver(t, map[string]map[string]any{
		"app-db": {"url": "postgres://scott:secret-tiger-pw@127.0.0.1:1/appdb"},
	})
	_, err := runSQL(t, sqlQueryNode("dead", func(n *SQLQuery) {
		n.Credential = "app-db"
		n.Query = "SELECT 1"
	}), nil)
	if err == nil {
		t.Fatal("dial to a closed port must fail")
	}
	if !workflow.IsRetryableError(err) {
		t.Fatalf("transport failure must classify retryable, got: %v", err)
	}
	if strings.Contains(err.Error(), "secret-tiger-pw") {
		t.Fatalf("credential password must never be echoed: %v", err)
	}
}

func TestSQLQueryPartsFormCredentialComposesPostgres(t *testing.T) {
	sqlTestResolver(t, map[string]map[string]any{
		"pg-parts": {
			"host": "127.0.0.1", "port": 1, "user": "scott", "password": "tiger", "database": "appdb",
			// parts form 缺省 driver = postgresql+psycopg2（database.py:101）
		},
	})
	_, err := runSQL(t, sqlQueryNode("parts", func(n *SQLQuery) {
		n.Credential = "pg-parts"
		n.Query = "SELECT 1"
	}), nil)
	if err == nil {
		t.Fatal("dial to a closed port must fail")
	}
	if !workflow.IsRetryableError(err) {
		t.Fatalf("parts-form composed DSN must reach the transport layer, got: %v", err)
	}
	if strings.Contains(err.Error(), "tiger") {
		t.Fatalf("parts-form password must never be echoed: %v", err)
	}
}

func TestSQLQueryIncompletePartsCredentialNamed(t *testing.T) {
	sqlTestResolver(t, map[string]map[string]any{
		"half": {"host": "h1", "port": 5432, "user": "u"}, // 缺 password/database
	})
	_, err := runSQL(t, sqlQueryNode("half", func(n *SQLQuery) {
		n.Credential = "half"
		n.Query = "SELECT 1"
	}), nil)
	if err == nil || !strings.Contains(err.Error(), "full host/port/user/password/database set") {
		t.Fatalf("want the named parts-form error (database.py:105), got %v", err)
	}
}

// --- 凭据面（resolver 钩子 + PLAITA_CREDENTIALS_FILE 文件钩子） ---

func TestSQLQueryCredentialFileHook(t *testing.T) {
	prev := sqlCredentialResolverOverride
	sqlCredentialResolverOverride = nil
	t.Cleanup(func() { sqlCredentialResolverOverride = prev })

	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.json")

	// 文件未配置：具名指路，绝不空 DSN 起拨。
	if _, err := runSQL(t, sqlQueryNode("n1", func(n *SQLQuery) {
		n.Credential = "x"
		n.Query = "SELECT 1"
	}), nil); err == nil || !strings.Contains(err.Error(), "PLAITA_CREDENTIALS_FILE") {
		t.Fatalf("unconfigured face must name the deployment hook, got %v", err)
	}

	// 权限过宽：fail-loud（A62-05 providers.json 同款纪律）。
	if err := os.WriteFile(path, []byte(`{"x": {"url": "sqlite://:memory:"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PLAITA_CREDENTIALS_FILE", path)
	if _, err := runSQL(t, sqlQueryNode("n2", func(n *SQLQuery) {
		n.Credential = "x"
		n.Query = "SELECT 1"
	}), nil); err == nil || !strings.Contains(err.Error(), "too permissive") {
		t.Fatalf("0644 credentials file must fail loud, got %v", err)
	}

	// 损坏文件：fail-loud，不当空 store（CredentialStoreCorruptError 同型）。
	// os.WriteFile 不改既有文件的权限位，先删再写保住 0600 断言路径。
	_ = os.Remove(path)
	if err := os.WriteFile(path, []byte(`{"x": `), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runSQL(t, sqlQueryNode("n3", func(n *SQLQuery) {
		n.Credential = "x"
		n.Query = "SELECT 1"
	}), nil); err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("corrupt credentials file must fail loud, got %v", err)
	}

	// 缺名：列持有量，不列他人载荷。
	if err := os.WriteFile(path, []byte(`{"other": {"url": "sqlite://:memory:"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runSQL(t, sqlQueryNode("n4", func(n *SQLQuery) {
		n.Credential = "x"
		n.Query = "SELECT 1"
	}), nil); err == nil || !strings.Contains(err.Error(), `credential "x" not found`) {
		t.Fatalf("missing name must be named, got %v", err)
	}

	// 正常路径：0600 + 命中条目。
	if err := os.WriteFile(path, []byte(`{"x": {"url": "`+sqlTestSQLiteURL("sqlunit_filehook")+`"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := runSQL(t, sqlQueryNode("n5", func(n *SQLQuery) {
		n.Credential = "x"
		n.Query = "SELECT 1 AS one"
	}), nil)
	if err != nil {
		t.Fatalf("file-hook resolve must work: %v", err)
	}
	if out["rowcount"] != 1 {
		t.Errorf("rowcount = %v", out["rowcount"])
	}
}

func TestSQLQueryTenantRefusedBeforeCredentialFace(t *testing.T) {
	sqlTestResolver(t, map[string]map[string]any{
		"app-db": {"url": sqlTestSQLiteURL("sqlunit_tenant")},
	})
	// resolver 装着也必须拒：闸在 resolver 之前。
	rc := &runtime.Context{
		WorkflowInput:      map[string]any{},
		ExecuteNodeResults: map[string]any{},
		EnvVariables:       map[string]any{},
		GlobalVariables:    map[string]any{},
	}
	ctx := runtime.WithWorkflowRuntimeContext(
		runtime.WithTenantID(context.Background(), "acme"), rc)

	_, err := runSQL(t, sqlQueryNode("t1", func(n *SQLQuery) {
		n.Credential = "app-db"
		n.Query = "SELECT 1"
	}), ctx)
	if err == nil {
		t.Fatal("non-default tenant must be refused")
	}
	if !errors.Is(err, ErrTenantIsolationUnavailable) {
		t.Fatalf("want ErrTenantIsolationUnavailable, got %v", err)
	}
	if workflow.IsRetryableError(err) {
		t.Errorf("tenant refusal must be NonRetryable")
	}
}

// --- 参数改写器（sqlalchemy text() 方言） ---

func TestRewriteNamedSQLParamsQMark(t *testing.T) {
	out, args, err := rewriteNamedSQLParams(
		"SELECT * FROM t WHERE a = :a AND b = :b AND a = :a", map[string]any{"a": 1, "b": "x"}, sqlPlaceholderQMark)
	if err != nil {
		t.Fatal(err)
	}
	if out != "SELECT * FROM t WHERE a = ? AND b = ? AND a = ?" {
		t.Errorf("out = %q", out)
	}
	if len(args) != 3 || args[0] != 1 || args[1] != "x" || args[2] != 1 {
		t.Errorf("args = %v (qmark repeats per occurrence)", args)
	}
}

func TestRewriteNamedSQLParamsDollar(t *testing.T) {
	out, args, err := rewriteNamedSQLParams(
		"SELECT * FROM t WHERE a = :a AND b = :b AND c = :a", map[string]any{"a": 1, "b": "x"}, sqlPlaceholderDollar)
	if err != nil {
		t.Fatal(err)
	}
	if out != "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $1" {
		t.Errorf("out = %q (repeated name reuses its number)", out)
	}
	if len(args) != 2 || args[0] != 1 || args[1] != "x" {
		t.Errorf("args = %v (first-appearance order, deduped)", args)
	}
}

func TestRewriteNamedSQLParamsSkipsNonBinds(t *testing.T) {
	// `\:` escape 在字符串字面量之外才有意义（串内反斜杠是字面字符，原样保留）。
	sql := "SELECT 'a:b :not' AS s, \"col:name\" AS c, x::text, /* :hidden */ y -- :also\n" +
		"FROM t WHERE payload = $$ {\"k\": \":novalue\"} $$ AND marker = \\:literal AND z = :real"
	out, args, err := rewriteNamedSQLParams(sql, map[string]any{"real": 7}, sqlPlaceholderQMark)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(out, "?") != 1 {
		t.Errorf("only :real may bind, out = %q", out)
	}
	if !strings.Contains(out, "'a:b :not'") || !strings.Contains(out, "x::text") {
		t.Errorf("literals/casts must survive verbatim: %q", out)
	}
	if !strings.Contains(out, ":literal") || strings.Contains(out, "\\:literal") {
		t.Errorf("escaped colon becomes a literal colon (backslash dropped): %q", out)
	}
	if !strings.Contains(out, `$$ {"k": ":novalue"} $$`) {
		t.Errorf("dollar-quoted body must survive: %q", out)
	}
	if len(args) != 1 || args[0] != 7 {
		t.Errorf("args = %v", args)
	}
}

func TestRewriteNamedSQLParamsMissingValueNamed(t *testing.T) {
	_, _, err := rewriteNamedSQLParams("SELECT :missing", map[string]any{}, sqlPlaceholderQMark)
	if err == nil || !strings.Contains(err.Error(), "no value for bind parameter :missing") {
		t.Fatalf("want named missing-bind error, got %v", err)
	}
}

// --- 语句分类 / 错误分类表 ---

func TestSQLIsWriteStatement(t *testing.T) {
	writes := []string{
		"INSERT INTO t VALUES (1)", "  -- lead comment\n  UPDATE t SET a=1",
		"/* c */ DELETE FROM t",
		"CREATE TABLE t(a int)", "TRUNCATE t", "SET search_path = public",
		"(INSERT INTO t VALUES (1))", "BEGIN",
	}
	for _, s := range writes {
		if !sqlIsWriteStatement(s) {
			t.Errorf("%q must classify write", s)
		}
	}
	reads := []string{
		"SELECT 1", "WITH x AS (SELECT 1) SELECT * FROM x", "SHOW TABLES",
		"EXPLAIN SELECT 1", "PRAGMA table_info(t)", "VALUES (1)",
		// WITH 按前导关键字走 rows 路径（可写 CTE 无 RETURNING 时零列 →
		// rowcount 0，documented divergence）。
		"WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
	}
	for _, s := range reads {
		if sqlIsWriteStatement(s) {
			t.Errorf("%q must classify read", s)
		}
	}
}

func TestSQLStatementRetryableTable(t *testing.T) {
	retryable := []error{
		fmt.Errorf("wrap: %w", netErrorStub()),
		context.DeadlineExceeded,
		driver.ErrBadConn,
		&pgconn.PgError{Code: "53300"},  // too_many_connections
		&pgconn.PgError{Code: "40001"},  // serialization_failure
		&pgconn.PgError{Code: "40P01"},  // deadlock_detected
		&pgconn.PgError{Code: "57P03"},  // cannot_connect_now
		&mysql.MySQLError{Number: 1213}, // deadlock
		&mysql.MySQLError{Number: 1040}, // too many connections
		&mysql.MySQLError{Number: 2003}, // can't connect
	}
	for _, err := range retryable {
		if !sqlStatementRetryable(err) {
			t.Errorf("%v must be retryable", err)
		}
	}
	deterministic := []error{
		context.Canceled,                // cancellation is not a retryable failure
		&pgconn.PgError{Code: "42601"},  // syntax_error
		&pgconn.PgError{Code: "28P01"},  // invalid_password
		&pgconn.PgError{Code: "57014"},  // query_canceled — cancellation, not retry
		&mysql.MySQLError{Number: 1064}, // syntax error
		&mysql.MySQLError{Number: 1045}, // access denied
		fmt.Errorf("some unknown driver failure"),
	}
	for _, err := range deterministic {
		if sqlStatementRetryable(err) {
			t.Errorf("%v must be deterministic (server-rejected SQL fails again on retry)", err)
		}
	}
}

func netErrorStub() error {
	return &net.OpError{Op: "dial", Err: errors.New("connection refused")}
}

// --- 连接池 LRU ---

func TestSQLPoolLRUEviction(t *testing.T) {
	sqlPoolMu.Lock()
	prevPools, prevOrder := sqlPools, sqlPoolOrder
	sqlPools, sqlPoolOrder = map[string]*sql.DB{}, nil
	sqlPoolMu.Unlock()
	t.Cleanup(func() {
		sqlPoolMu.Lock()
		for _, db := range sqlPools {
			_ = db.Close()
		}
		sqlPools, sqlPoolOrder = prevPools, prevOrder
		sqlPoolMu.Unlock()
	})

	for i := 0; i < sqlPoolCacheMax+2; i++ {
		target := sqlTarget{driver: "sqlite", dsn: fmt.Sprintf("file:sqlunit_lru%d?mode=memory", i), family: sqlFamilySQLite}
		if _, err := sqlPoolFor(target); err != nil {
			t.Fatalf("pool %d: %v", i, err)
		}
	}
	sqlPoolMu.Lock()
	defer sqlPoolMu.Unlock()
	if len(sqlPools) != sqlPoolCacheMax {
		t.Errorf("pool cache = %d, want bounded %d", len(sqlPools), sqlPoolCacheMax)
	}
}
