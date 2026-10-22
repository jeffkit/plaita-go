package nodes

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
)

// Credential payload → database/sql target, mirroring database.py's
// `_compose_url` + `_get_engine` (database.py:78-107): the payload carries
// either {"url": …} or the host/port/user/password/database parts with an
// optional driver tag (default "postgresql+psycopg2"). SQLAlchemy URL schemes
// are accepted verbatim and mapped onto the compiled driver matrix — flows
// written against the Python node keep their spellings.
//
// The engine cache (`_engine_cache` keyed by full URL) becomes a bounded LRU
// of *sql.DB pools keyed by driver+DSN; evicted pools are closed. Errors and
// logs never carry the DSN (the red line shared with the redis node): the
// redacted identity below is the only thing that may appear in a message.

type sqlFamily int

const (
	sqlFamilyPostgres sqlFamily = iota
	sqlFamilyMySQL
	sqlFamilySQLite
)

type sqlTarget struct {
	driver     string
	dsn        string // cache key material; never logged, never echoed
	identity   string // redacted: "postgres://host:5432/appdb" / "sqlite:<path>"
	family     sqlFamily
	paramStyle sqlParamStyle
}

// sqlSchemeFamilies maps the SQLAlchemy scheme spellings onto the driver
// matrix (sql_drivers.go). Unknown schemes are a named error listing what is
// supported — same failure shape as sqlalchemy's NoSuchModuleError, minus
// the echo of the offending URL.
var sqlSchemeFamilies = map[string]sqlFamily{
	"postgres":             sqlFamilyPostgres,
	"postgresql":           sqlFamilyPostgres,
	"postgres+pgx":         sqlFamilyPostgres,
	"postgresql+psycopg2":  sqlFamilyPostgres,
	"postgresql+psycopg":   sqlFamilyPostgres,
	"postgresql+pg8000":    sqlFamilyPostgres,
	"postgresql+pgx":       sqlFamilyPostgres,
	"mysql":                sqlFamilyMySQL,
	"mysql+mysqldb":        sqlFamilyMySQL,
	"mysql+pymysql":        sqlFamilyMySQL,
	"mysql+cymysql":        sqlFamilyMySQL,
	"mysql+mysqlconnector": sqlFamilyMySQL,
	"sqlite":               sqlFamilySQLite,
	"sqlite+pysqlite":      sqlFamilySQLite,
	"sqlite3":              sqlFamilySQLite,
}

const sqlPoolCacheMax = 8 // same bound as the redis client LRU

var (
	sqlPoolMu    sync.Mutex
	sqlPools     = map[string]*sql.DB{}
	sqlPoolOrder []string
)

// sqlTargetFromCredential builds the connection target from a resolved
// credential payload: url form wins (database.py:93 `if url: return url`),
// else the host/port/user/password/database parts must be complete
// (database.py:105 "凭据数据需提供 url，或 host/port/user/password/database 全集").
func sqlTargetFromCredential(payload map[string]any) (sqlTarget, error) {
	if raw, ok := payload["url"]; ok && raw != nil {
		s, isStr := raw.(string)
		if !isStr {
			return sqlTarget{}, fmt.Errorf("credential field \"url\" must be a string")
		}
		return sqlTargetFromURL(s)
	}

	host, hasHost := sqlStringField(payload, "host")
	port, hasPort := sqlStringField(payload, "port")
	user, hasUser := sqlStringField(payload, "user")
	password, hasPassword := sqlStringField(payload, "password")
	database, hasDatabase := sqlStringField(payload, "database")
	if !hasHost || !hasPort || !hasUser || !hasPassword || !hasDatabase {
		return sqlTarget{}, fmt.Errorf(
			"credential data must provide either \"url\" or the full host/port/user/password/database set")
	}
	driverTag, _ := sqlStringField(payload, "driver")
	family, err := sqlFamilyForDriverTag(driverTag)
	if err != nil {
		return sqlTarget{}, err
	}

	identity := fmt.Sprintf("%s://%s:%s/%s", sqlRedactedScheme(family), host, port, database)
	switch family {
	case sqlFamilyPostgres:
		// database.py:100 composes scheme://user:pass@host:port/db; pgx takes
		// URL-form DSNs natively (net/url escapes the userinfo).
		u := url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(user, password),
			Host:   net.JoinHostPort(host, port),
			Path:   "/" + database,
		}
		return sqlTarget{driver: "pgx", dsn: u.String(), identity: identity, family: family, paramStyle: sqlPlaceholderDollar}, nil
	case sqlFamilyMySQL:
		cfg := mysql.NewConfig()
		cfg.User = user
		cfg.Passwd = password
		cfg.Net = "tcp"
		cfg.Addr = net.JoinHostPort(host, port)
		cfg.DBName = database
		cfg.ParseTime = true // pymysql converts DATETIME to datetime unconditionally
		return sqlTarget{driver: "mysql", dsn: cfg.FormatDSN(), identity: identity, family: family, paramStyle: sqlPlaceholderQMark}, nil
	default:
		return sqlTarget{}, fmt.Errorf(
			"parts-form credentials need a host-based driver (postgres/mysql); sqlite takes url-form credentials")
	}
}

// sqlTargetFromURL parses an SQLAlchemy-style URL into the driver matrix.
func sqlTargetFromURL(raw string) (sqlTarget, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		// err text embeds the offending URL — never propagate it.
		return sqlTarget{}, fmt.Errorf("credential \"url\" is not a valid connection URL")
	}
	family, ok := sqlSchemeFamilies[strings.ToLower(u.Scheme)]
	if !ok {
		return sqlTarget{}, fmt.Errorf(
			"unsupported connection scheme %q: supported families are postgres (pgx), mysql, sqlite", u.Scheme)
	}
	identity := sqlRedactedURLIdentity(family, u)

	switch family {
	case sqlFamilyPostgres:
		// pgx parses postgres:// / postgresql:// URLs natively; strip the
		// sqlalchemy driver suffix (postgresql+psycopg2:// etc.).
		u.Scheme = "postgres"
		if _, err := pgx.ParseConfig(u.String()); err != nil {
			// ParseConfigError embeds the connect string — replace it wholesale.
			return sqlTarget{}, fmt.Errorf("invalid postgres connection settings (host/port/database or params rejected)")
		}
		return sqlTarget{driver: "pgx", dsn: u.String(), identity: identity, family: family, paramStyle: sqlPlaceholderDollar}, nil
	case sqlFamilyMySQL:
		cfg := mysql.NewConfig()
		if u.User != nil {
			cfg.User = u.User.Username()
			cfg.Passwd, _ = u.User.Password()
		}
		_, port := "", ""
		if u.Port() != "" {
			port = u.Port()
		} else {
			port = "3306"
		}
		cfg.Net = "tcp"
		cfg.Addr = net.JoinHostPort(u.Hostname(), port)
		cfg.DBName = strings.TrimPrefix(u.Path, "/")
		cfg.ParseTime = true
		for k, vs := range u.Query() {
			if len(vs) == 0 {
				continue
			}
			if k == "parseTime" {
				cfg.ParseTime = vs[0] == "true" || vs[0] == "1"
				continue
			}
			if cfg.Params == nil {
				cfg.Params = map[string]string{}
			}
			cfg.Params[k] = vs[0]
		}
		dsn := cfg.FormatDSN()
		if _, err := mysql.ParseDSN(dsn); err != nil {
			// *mysql.InvalidDSNError embeds the DSN — replace it wholesale.
			return sqlTarget{}, fmt.Errorf("invalid mysql connection settings (host/port/database or params rejected)")
		}
		return sqlTarget{driver: "mysql", dsn: dsn, identity: identity, family: family, paramStyle: sqlPlaceholderQMark}, nil
	default: // sqlite
		dsn := sqlSQLiteDSN(u)
		return sqlTarget{driver: "sqlite", dsn: dsn, identity: "sqlite:" + dsn, family: family, paramStyle: sqlPlaceholderQMark}, nil
	}
}

// sqlSQLiteDSN maps the SQLAlchemy sqlite spellings onto modernc DSNs:
// "sqlite://" (memory), "sqlite:///:memory:", "sqlite:///relative.db",
// "sqlite:////absolute.db", "sqlite://:memory:", and the opaque forms
// "sqlite::memory:" / "sqlite:file:cache.db?mode=memory" (a "file:…" authority
// cannot be spelled as //host — url.Parse would read it as host:port).
func sqlSQLiteDSN(u *url.URL) string {
	dsn := ""
	switch {
	case u.Opaque != "":
		dsn = u.Opaque
	case u.Host != "":
		dsn = u.Host + u.Path
	default:
		dsn = strings.TrimPrefix(u.Path, "/")
	}
	if dsn == "" {
		dsn = ":memory:"
	}
	if u.RawQuery != "" {
		// modernc takes query params on file: URIs; a DSN already spelled
		// "file:…" must not be doubled.
		if !strings.HasPrefix(dsn, "file:") {
			dsn = "file:" + dsn
		}
		dsn = dsn + "?" + u.RawQuery
	}
	return dsn
}

func sqlFamilyForDriverTag(tag string) (sqlFamily, error) {
	// database.py:101 defaults to "postgresql+psycopg2" when the parts form
	// carries no driver tag.
	if strings.TrimSpace(tag) == "" {
		return sqlFamilyPostgres, nil
	}
	family, ok := sqlSchemeFamilies[strings.ToLower(strings.TrimSpace(tag))]
	if !ok {
		return 0, fmt.Errorf(
			"unsupported credential driver %q: supported families are postgres (pgx), mysql, sqlite", tag)
	}
	return family, nil
}

// sqlRedactedScheme names the family for identity strings without touching
// credential content.
func sqlRedactedScheme(family sqlFamily) string {
	switch family {
	case sqlFamilyMySQL:
		return "mysql"
	case sqlFamilySQLite:
		return "sqlite"
	default:
		return "postgres"
	}
}

// sqlRedactedURLIdentity reduces a credential URL to scheme://host:port/db —
// userinfo and query are dropped (the password rides in both for URL DSNs;
// sslmode etc. are not worth echoing).
func sqlRedactedURLIdentity(family sqlFamily, u *url.URL) string {
	scheme := sqlRedactedScheme(family)
	if family == sqlFamilySQLite {
		return scheme + ":" + sqlSQLiteDSN(&url.URL{Host: u.Host, Path: u.Path})
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		if family == sqlFamilyMySQL {
			port = "3306"
		} else {
			port = "5432"
		}
	}
	return fmt.Sprintf("%s://%s:%s%s", scheme, host, port, u.Path)
}

// sqlPoolFor returns a pooled *sql.DB for the target: same target reuses the
// same pool across executions (the `_get_engine` cache, database.py:78), the
// LRU bound closes evicted pools. Database/sql transparently retries stale
// pooled connections (driver ErrBadConn → fresh dial), which covers the
// pool_pre_ping=True posture of the Python engine cache.
func sqlPoolFor(t sqlTarget) (*sql.DB, error) {
	key := t.driver + "\x00" + t.dsn
	sqlPoolMu.Lock()
	defer sqlPoolMu.Unlock()
	if db, ok := sqlPools[key]; ok {
		return db, nil
	}
	db, err := sql.Open(t.driver, t.dsn)
	if err != nil {
		// sql.Open only rejects unknown driver names; nothing to echo.
		return nil, fmt.Errorf("sql_query: cannot open %s driver: %w", t.driver, err)
	}
	sqlPools[key] = db
	sqlPoolOrder = append(sqlPoolOrder, key)
	for len(sqlPoolOrder) > sqlPoolCacheMax {
		oldest := sqlPoolOrder[0]
		sqlPoolOrder = sqlPoolOrder[1:]
		if evicted, ok := sqlPools[oldest]; ok {
			delete(sqlPools, oldest)
			_ = evicted.Close()
		}
	}
	return db, nil
}

// sqlCellGoValue normalizes a scanned column value for JSON output:
// []byte (TEXT columns on several drivers) becomes a string, and driver
// wrapper types (pgx's pgtype.Numeric and friends implement driver.Valuer)
// collapse to their underlying value — the closest Go shape to Python's
// `dict(row._mapping)` with default=str stringification happening at encode
// time (json.Marshal renders time.Time as RFC3339 where Python str() would
// render its own format; both keep values human-readable instead of crashing
// the row encode).
func sqlCellGoValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case time.Time:
		return t
	default:
	}
	if valuer, ok := v.(driver.Valuer); ok {
		if underlying, err := valuer.Value(); err == nil && underlying != nil {
			return sqlCellGoValue(underlying)
		}
	}
	return v
}
