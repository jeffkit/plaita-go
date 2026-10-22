package nodes

// #84 driver matrix (decision recorded in AGENTS.md): database/sql plus three
// pure-Go drivers, no cgo — the worker builds on hosts without a C toolchain
// and stays cross-compilable, same posture as the rest of this repo. Python's
// sqlalchemy is an optional extra (database.py lazy-imports it); Go cannot
// make imports optional, so the drivers are compiled in and the scheme map in
// sql_dsn.go is the single place that knows their names:
//
//	postgresql family → github.com/jackc/pgx/v5/stdlib (registers "pgx"; pure Go)
//	mysql family      → github.com/go-sql-driver/mysql (registers "mysql"; pure Go)
//	sqlite family     → modernc.org/sqlite (registers "sqlite"; pure Go, no cgo)
//
// Version pins sit at the go 1.24 line of go.mod on purpose: pgx v5.8.0 (the
// last release requiring only go 1.24) and modernc.org/sqlite v1.38.0 (the
// last of its series on go 1.23) — newer releases demand go 1.25/1.26 and
// would force a toolchain bump for the whole engine.
import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)
