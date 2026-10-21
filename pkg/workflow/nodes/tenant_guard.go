package nodes

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// TenantIsolationMarker mirrors plaita-nodes _tenant_guard.TENANT_ISOLATION_MARKER
// and plaita/node/code.py _SUBPROCESS_TENANT_ISOLATION_MARKER: the named marker
// operators grep for when a tenant-scoped host command was refused (pn#18 /
// A42-06).
const TenantIsolationMarker = "PLAITA_SUBPROCESS_TENANT_ISOLATION_UNAVAILABLE"

// ErrTenantIsolationUnavailable is the fail-closed refusal shared by the host
// subprocess nodes (gate / capture / code python): a non-default tenant's
// command would run as the worker user with unrestricted host file-system
// access, and every tenant's credentials are encrypted with the same key, so
// passing the tenant id down would only change routing, not the boundary. The
// command is refused before it is spawned (zero execution chance); there is no
// host-run opt-out in this engine. Tenant-scoped commands belong in a container
// that does not mount host credentials (plaita's docker backend).
var ErrTenantIsolationUnavailable = errors.New(TenantIsolationMarker)

// refuseNonDefaultTenantReasoned is the shared refusal core behind every
// tenant gate in this package (#34 subprocess family, #36 host write face,
// #37 redis platform state): a non-default tenant gets the family sentinel
// (ErrTenantIsolationUnavailable, carrying the public marker), the refusal is
// deterministic (NonRetryable — retrying cannot change the tenant and must
// not burn the retry budget), and the log line is observable without echoing
// a path or credential. Empty tenant (bare engine / single-tenant deployment)
// and "default" never reach the refusal.
func refuseNonDefaultTenantReasoned(ctx context.Context, nodeType, reason string) error {
	tenant := runtime.TenantIDFromContext(ctx)
	if !runtime.IsNonDefaultTenant(tenant) {
		return nil
	}
	// Fail-closed gates must be observable (degradations are never silent in
	// this engine); the error itself also lands on the execution event stream.
	log.Printf("%s refused non-default tenant=%s (%s); not executed", nodeType, tenant, TenantIsolationMarker)
	return workflow.NonRetryable(fmt.Errorf(
		"%w: %s refused for tenant %q: %s",
		ErrTenantIsolationUnavailable, nodeType, tenant, reason))
}

// refuseNonDefaultTenant is the shared tenant gate for host-executing
// subprocess nodes, porting plaita-nodes _tenant_guard.refuse_non_default_tenant.
// An empty tenant id is the bare-engine / single-tenant deployment and passes
// (plaita's current_tenant() falls back to DEFAULT_TENANT_ID there).
//
// nodeType labels the refusal ("gate", "capture", "code python") — the
// message and the log line carry the tenant but never a file-system path.
func refuseNonDefaultTenant(ctx context.Context, nodeType string) error {
	tenant := runtime.TenantIDFromContext(ctx)
	if !runtime.IsNonDefaultTenant(tenant) {
		return nil
	}
	return refuseNonDefaultTenantReasoned(ctx, nodeType, fmt.Sprintf(
		"the subprocess cannot isolate credentials: the child would run "+
			"as the worker user with unrestricted host file-system access, and all tenants "+
			"share one credentials key, so its command could read the default tenant's "+
			"credentials; refusing to execute. Run tenant-scoped commands in a container "+
			"that does not mount host credentials (plaita docker backend); the default "+
			"tenant keeps the historical semantics"))
}

// refuseNonDefaultTenantWriteFace gates the flow-controlled host write face
// (#36): writefile / git_publish / report / rate_limit / github_comment. What
// the node would do is stated per node ("the node would write host files"),
// the shared suffix carries the policy: a tenant's flow gets no host write
// face in this engine (no opt-out — the write lands as the worker user on a
// host shared by every tenant); the default tenant keeps the historical
// semantics.
func refuseNonDefaultTenantWriteFace(ctx context.Context, nodeType, what string) error {
	return refuseNonDefaultTenantReasoned(ctx, nodeType, what+
		" as the worker user on the host shared by every tenant; tenant flows get no "+
		"host write face in this engine and there is no opt-out; run tenant-scoped "+
		"writes in a container that does not mount host credentials (plaita docker "+
		"backend); the default tenant keeps the historical semantics")
}
