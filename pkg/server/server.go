// Package server implements the console BFF read-only subset (plaita-go #33):
// execution queries and the execution event-stream read side, served over
// stdlib net/http with zero new dependencies.
//
// Read-only by construction: only GET routes are registered (anything else
// answers 405), and the package performs no writes to Redis — execution
// state, node timelines and events are read through the worker package's
// ExecutionState persistence (§5) and the persisted timeline half of the
// event double-write (§6.2). Cancel/retry/start stay console-side (keeper /
// queue carriers); this BFF will never grow write endpoints.
//
// Contract alignment: field names, paging shape and error codes follow the
// Python console BFF (plaita-console/backend/api/executions.py —
// ExecutionInfo / ExecutionListResponse / _timeline_envelope). Deliberate
// deviations are called out at each site.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/worker"
)

// contract limits mirrored from the Python BFF (api/executions.py).
const (
	// maxListSize is EXECUTION_LIST_MAX_SIZE: a larger ?size= answers 422.
	maxListSize = 200
	// maxPage is the Python `page: le=10_000` bound.
	maxPage = 10000
	// defaultListSize is the Python `size: default=20`.
	defaultListSize = 20
	// filterMaxCandidates is EXECUTION_FILTER_MAX_CANDIDATES: the filtered
	// list never projects more state rows than this per request; hitting the
	// cap answers truncated=true with total as a matched-count lower bound.
	filterMaxCandidates = 10000
	// defaultEventsLimit / maxEventsLimit bound GET /executions/{id}/events.
	// The cap equals the timeline cap (500, §5.1): one full page can carry
	// every event the execution still retains.
	defaultEventsLimit = 100
	maxEventsLimit     = 500
)

// Identity is the authenticated caller view injected by an Authenticator.
//
// TenantID is the tenant scope: it routes every read through
// worker.Config.Namespace (§3.1: the default/empty tenant uses the plain
// prefix, any other tenant lives under prefix:{tenant}). The empty TenantID
// means the default tenant — NOT a platform-wide view. Unlike the Python
// console (tenant_scope=None fans out over all namespaces), this BFF always
// answers from exactly one namespace, which is what makes tenant isolation
// structural rather than filter-shaped.
type Identity struct {
	// TenantID scopes every read; "" = default tenant.
	TenantID string
	// Subject is an opaque caller label, log-only.
	Subject string
}

// Authenticator authenticates an incoming request. Returning an error
// rejects the request with 401 {"detail": ...}; returning an Identity lets
// it through, scoped to Identity.TenantID.
type Authenticator interface {
	Authenticate(r *http.Request) (*Identity, error)
}

// AuthenticatorFunc adapts a function to the Authenticator interface.
type AuthenticatorFunc func(r *http.Request) (*Identity, error)

// Authenticate implements Authenticator.
func (f AuthenticatorFunc) Authenticate(r *http.Request) (*Identity, error) {
	return f(r)
}

// TenantHeader carries the tenant scope in local-trust mode. It is only
// honored by LocalTrustAuthenticator; a real Authenticator decides its own
// way to bind tenants (token claims, mTLS, ...) and should ignore it.
const TenantHeader = "X-Plaita-Tenant"

// LocalTrustAuthenticator is the zero-config default: every caller is
// trusted and the tenant scope comes from the X-Plaita-Tenant header
// (empty → default tenant). This is a deployment statement — the server
// must only be reachable from a trusted network — and it is made visible:
// every response carries `X-Plaita-Auth: unauthenticated` and every request
// is logged with auth=unauthenticated, so a mis-exposed instance is
// observable in access logs.
type LocalTrustAuthenticator struct{}

// Authenticate implements Authenticator.
func (LocalTrustAuthenticator) Authenticate(r *http.Request) (*Identity, error) {
	return &Identity{
		TenantID: strings.TrimSpace(r.Header.Get(TenantHeader)),
		Subject:  "local-trust",
	}, nil
}

// Server is the read-only console BFF. It owns no storage: state reads go
// through worker.NewExecutionStore and the execution index ZSET documented
// in docs/WORKER_PROTOCOL.md §5.2 (member = `{ns}:execution:{id}`, score =
// start_time epoch).
type Server struct {
	cfg   worker.Config
	rdb   *redis.Client
	store *worker.ExecutionStore
	auth  Authenticator
	// trustMode marks the default no-auth wiring; it drives the
	// `X-Plaita-Auth: unauthenticated` response annotation.
	trustMode bool
	logger    *slog.Logger
	mux       *http.ServeMux
	// contract knobs (Python-aligned defaults; fields so tests/hosts can
	// tighten them).
	listMaxSize    int
	filterCap      int
	eventsMaxLimit int
}

// Option configures the Server.
type Option func(*Server)

// WithAuthenticator installs a custom authenticator. Without it the server
// runs in local-trust mode (LocalTrustAuthenticator, annotated responses).
func WithAuthenticator(a Authenticator) Option {
	return func(s *Server) { s.auth, s.trustMode = a, false }
}

// WithLogger installs the request/error logger (default: slog default).
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) { s.logger = l }
}

// New builds the read-only BFF over the worker's Redis namespace. The
// config is cloned and only the namespace-bearing fields are used; a nil
// config falls back to worker.DefaultConfig().
func New(cfg *worker.Config, rdb *redis.Client, opts ...Option) *Server {
	s := &Server{rdb: rdb, trustMode: true}
	if cfg == nil {
		cfg = worker.DefaultConfig()
	}
	s.cfg = *cfg
	if s.cfg.KeyPrefix == "" {
		s.cfg.KeyPrefix = worker.DefaultKeyPrefix
	}
	if s.cfg.DefaultTenantID == "" {
		s.cfg.DefaultTenantID = "default"
	}
	s.store = worker.NewExecutionStore(rdb, &s.cfg)
	s.logger = slog.Default()
	s.listMaxSize = maxListSize
	s.filterCap = filterMaxCandidates
	s.eventsMaxLimit = maxEventsLimit
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	if s.auth == nil {
		// Default = local trust mode, explicitly annotated on every response.
		s.auth = LocalTrustAuthenticator{}
		s.trustMode = true
	}

	mux := http.NewServeMux()
	// Method-scoped patterns: non-GET verbs answer 405 from the mux itself,
	// which is the machine-checkable form of "read-only, no write endpoints".
	mux.HandleFunc("GET /executions", s.handleListExecutions)
	mux.HandleFunc("GET /executions/{id}", s.handleGetExecution)
	mux.HandleFunc("GET /executions/{id}/events", s.handleExecutionEvents)
	s.mux = mux
	return s
}

// Handler returns the authenticated, logging http.Handler.
func (s *Server) Handler() http.Handler { return s }

// ServeHTTP authenticates the request, annotates the auth mode and
// dispatches to the route mux.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ident, err := s.auth.Authenticate(r)
	if err != nil {
		msg := err.Error()
		if msg == "" {
			msg = "unauthenticated"
		}
		s.logger.Warn("request rejected by authenticator", "path", r.URL.Path, "error", msg)
		writeError(w, http.StatusUnauthorized, msg)
		return
	}
	if ident == nil {
		ident = &Identity{}
	}
	if s.trustMode {
		// Local-trust deployments must be visible as such from the outside.
		w.Header().Set("X-Plaita-Auth", "unauthenticated")
	} else {
		w.Header().Set("X-Plaita-Auth", "authenticated")
	}
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	s.mux.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), identityKey{}, ident)))
	s.logger.Info("request",
		"method", r.Method, "path", r.URL.Path,
		"status", rec.status, "tenant", scopeTenant(ident.TenantID, s.cfg.DefaultTenantID),
		"subject", ident.Subject,
		"auth", map[bool]string{true: "unauthenticated", false: "authenticated"}[s.trustMode])
}

// statusRecorder captures the status code for the request log.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

type identityKey struct{}

func identityFrom(r *http.Request) *Identity {
	ident, _ := r.Context().Value(identityKey{}).(*Identity)
	if ident == nil {
		return &Identity{}
	}
	return ident
}

// scopeTenant renders the tenant for logs and the tenant_id field: the
// default/empty scope answers as "default" (Python _tenant_from_key).
func scopeTenant(tenant, defaultTenant string) string {
	if tenant == "" || tenant == defaultTenant {
		return "default"
	}
	return tenant
}

// tenantID is the identity's tenant scope (may be "" — the store's
// Namespace maps it to the plain prefix).
func identityTenant(r *http.Request) string { return identityFrom(r).TenantID }

// writeJSON emits one JSON body with the given status.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

// writeError emits the Python FastAPI error shape: {"detail": "..."}.
func writeError(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}

// writeInternal maps a state-read failure onto 500. The error text is
// logged, never leaked into the response (same scrubbing posture as the
// console's execution_data_parse_failed).
func (s *Server) writeInternal(w http.ResponseWriter, r *http.Request, what string, err error) {
	s.logger.Error("execution read failed", "what", what, "path", r.URL.Path, "error", err)
	writeError(w, http.StatusInternalServerError, "execution data read failed")
}

// notFound is the Python 404 shape, verbatim detail format included.
func notFound(w http.ResponseWriter, executionID string) {
	writeError(w, http.StatusNotFound, fmt.Sprintf("执行不存在: %s", executionID))
}

// queryInt parses one integer query parameter with default/bounds; out-of
// range or unparseable values answer 422 like FastAPI's Query(ge=..., le=...).
func queryInt(r *http.Request, name string, def, min, max int) (int, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return def, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min || n > max {
		return 0, false
	}
	return n, true
}

// queryInt64 is queryInt for the 64-bit incremental cursor.
func queryInt64(r *http.Request, name string, def, min int64) (int64, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return def, true
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < min {
		return 0, false
	}
	return n, true
}
