package ir

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #84：sql_query 转正后的 capability / IR 接受面 + 四条负控（先红后绿）。
//
// 本文件的负控刻意只用公开 API（ir.Load + wf.Execute + PLAITA_CREDENTIALS_FILE
// 凭据文件钩子）：基线（节点未实现、白名单未收）上四条全部红在同一签名——
// `node type "sql_query" is not supported by this engine`（ir.Load 拒载，
// 实测逐条记录在 issue 回评）；post-fix 全绿。细粒度语义（具名上限错误字段、
// 脱敏红线、分类表）在 pkg/workflow/nodes/sql_query_test.go（新 API/新符号，
// 基线为编译级红，接受面固有的先例形态）。

func TestCapabilityIncludesSQLQuery(t *testing.T) {
	cap := CapabilityManifest()
	supported := map[string]bool{}
	for _, t2 := range cap.SupportedNodeTypes {
		supported[t2] = true
	}
	assert.True(t, supported["sql_query"], "sql_query must be in the supported list after #84")
	assert.NotContains(t, cap.UnsupportedNodeTypes, "sql_query")
}

// sqlQueryNegRedEnv writes the plaintext credentials file the engine-level
// negative controls resolve through (the deployment hook, no test-only symbols).
func sqlQueryNegRedEnv(t *testing.T, payload string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "credentials.json")
	require.NoError(t, os.WriteFile(path, []byte(payload), 0o600))
	t.Setenv("PLAITA_CREDENTIALS_FILE", path)
}

const sqlQueryNegRedSQLiteURL = "sqlite:file:sqlneg84?mode=memory&cache=shared"

// sqlQueryNegRedSetup prepares the shared in-memory sqlite database the
// negative-control flows query. The handle stays open for the whole test so
// the shared-cache memory DB outlives individual pool connections.
func sqlQueryNegRedSetup(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:sqlneg84?mode=memory&cache=shared")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS sqlneg_basic (id INTEGER PRIMARY KEY, name TEXT)`,
		`DELETE FROM sqlneg_basic`,
		`INSERT INTO sqlneg_basic (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')`,
		`CREATE TABLE IF NOT EXISTS sqlneg_rows (id INTEGER PRIMARY KEY, note TEXT)`,
		`DELETE FROM sqlneg_rows`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("setup %q: %v", stmt, err)
		}
	}
	for i := 1; i <= 5; i++ {
		if _, err := db.Exec(`INSERT INTO sqlneg_rows (id, note) VALUES (?, ?)`, i, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("seed sqlneg_rows: %v", err)
		}
	}
	return db
}

func sqlQueryNegRedFlow(nodeJSON string) []byte {
	return []byte(fmt.Sprintf(`{
		"flow_id": "sql_query_negred",
		"runtime": "python",
		"inputType": {"dataType": "any"},
		"nodes": [
			{"type": "start", "id": "s", "next": "q"},
			%s,
			{"type": "end", "id": "o", "resultType": "success", "output": {"rc": "$NODE.q.rowcount", "rows": "$NODE.q.rows"}}
		]
	}`, nodeJSON))
}

// 负控 1（基本查询）：SELECT 形状 {"rows", "rowcount"} 与 Python 对拍。
func TestSQLQueryNegRedBasicSelect(t *testing.T) {
	sqlQueryNegRedEnv(t, fmt.Sprintf(`{"app-db": {"url": %q}}`, sqlQueryNegRedSQLiteURL))
	sqlQueryNegRedSetup(t)

	wf, err := Load(sqlQueryNegRedFlow(
		`{"type": "sql_query", "id": "q", "credential": "app-db", "query": "SELECT id, name FROM sqlneg_basic ORDER BY id", "next": "o"}`))
	require.NoError(t, err)
	result, execErr := wf.Execute(context.Background(), map[string]any{})
	require.NoError(t, execErr)

	out, ok := result.(map[string]any)
	require.True(t, ok, "result type %T", result)
	// $NODE 投影走表达式引擎，值保持原生 Go 形（rowcount=int、rows=原生切片）。
	assert.Equal(t, 3, out["rc"], "rowcount = len(rows) (database.py:282)")
	rows, ok := out["rows"].([]map[string]any)
	require.True(t, ok, "rows type %T", out["rows"])
	require.Len(t, rows, 3)
	assert.Equal(t, "alice", rows[0]["name"])
}

// 负控 2（参数化）：:name 绑定参数防注入路径（database.py:239 text(sql) + bind）。
func TestSQLQueryNegRedParameterized(t *testing.T) {
	sqlQueryNegRedEnv(t, fmt.Sprintf(`{"app-db": {"url": %q}}`, sqlQueryNegRedSQLiteURL))
	sqlQueryNegRedSetup(t)

	wf, err := Load(sqlQueryNegRedFlow(
		`{"type": "sql_query", "id": "q", "credential": "app-db", "query": "SELECT id FROM sqlneg_basic WHERE name = :who", "params": {"who": "bob"}, "next": "o"}`))
	require.NoError(t, err)
	result, execErr := wf.Execute(context.Background(), map[string]any{})
	require.NoError(t, execErr)

	out := result.(map[string]any)
	assert.Equal(t, 1, out["rc"])
	rows := out["rows"].([]map[string]any)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(2), rows[0]["id"])
}

// 负控 3（行上限截断具名）：max_rows=2 对 5 行数据 → 具名 fail-closed 错误、
// 无部分结果（plaita#205 / database.py SqlResultLimitError）。
func TestSQLQueryNegRedRowLimitNamed(t *testing.T) {
	sqlQueryNegRedEnv(t, fmt.Sprintf(`{"app-db": {"url": %q}}`, sqlQueryNegRedSQLiteURL))
	sqlQueryNegRedSetup(t)

	wf, err := Load(sqlQueryNegRedFlow(
		`{"type": "sql_query", "id": "q", "credential": "app-db", "query": "SELECT id, note FROM sqlneg_rows ORDER BY id", "max_rows": 2, "next": "o"}`))
	require.NoError(t, err)
	_, execErr := wf.Execute(context.Background(), map[string]any{})
	require.Error(t, execErr)
	assert.Contains(t, execErr.Error(), "result exceeds limits (max_rows=2")
	assert.Contains(t, execErr.Error(), "fail-closed")
	assert.NotContains(t, execErr.Error(), "row-3", "no partial result may leak through the error path")
}

// 负控 4（连接失败错误分类）：postgres DSN 打死端口 → 可重试面（transport
// family），且凭据里的密码绝不回显（userinfo 脱敏红线，负控可断言）。
func TestSQLQueryNegRedConnectionFailureClassifiedRetryable(t *testing.T) {
	sqlQueryNegRedEnv(t, `{"app-db": {"url": "postgres://scott:secret-tiger-pw@127.0.0.1:1/appdb"}}`)

	wf, err := Load(sqlQueryNegRedFlow(
		`{"type": "sql_query", "id": "q", "credential": "app-db", "query": "SELECT 1", "next": "o"}`))
	require.NoError(t, err)
	_, execErr := wf.Execute(context.Background(), map[string]any{})
	require.Error(t, execErr)
	// retryable: the transport family marker from workflow.RetryableError —
	// the engine retry loop is the consumer of this classification.
	assert.True(t, strings.HasPrefix(execErr.Error(), "retryable: "),
		"dial failure must classify retryable, got: %v", execErr)
	assert.NotContains(t, execErr.Error(), "secret-tiger-pw", "credential passwords must never be echoed")
}

// IR 接受面：sql_query 节点字段按 pydantic 拼写（snake）载入。
func TestSQLQueryIRLoadAccepts(t *testing.T) {
	flow := []byte(`{
		"flow_id": "sql_query_accept",
		"runtime": "python",
		"inputType": {"dataType": "any"},
		"nodes": [
			{"type": "start", "id": "s", "next": "q"},
			{"type": "sql_query", "id": "q", "credential": "app-db", "query": "SELECT 1",
			 "params": {"a": 1}, "dry_run": true, "max_rows": 10, "max_bytes": 2048, "next": "o"},
			{"type": "end", "id": "o", "resultType": "success", "output": {"d": "$NODE.q.dry_run", "rc": "$NODE.q.rowcount"}}
		]
	}`)
	wf, err := Load(flow)
	require.NoError(t, err)

	// dry-run 短路：凭据面未配置也必须成功（database.py:200-202 不解析凭据不连库）。
	result, execErr := wf.Execute(context.Background(), map[string]any{})
	require.NoError(t, execErr)
	raw, err := json.Marshal(result)
	require.NoError(t, err)
	assert.JSONEq(t, `{"d": true, "rc": 0}`, string(raw))
}
