package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
)

// Credential face for sql_query — the Go counterpart of
// plaita.credentials.get_credential (database.py:208 `get_credential(self.credential)`).
//
// Python reads a Fernet-encrypted JSON store exported by the console
// (PLAITA_CREDENTIALS_FILE + per-tenant side-files). The Go worker does not
// read that store (no Fernet, no per-tenant side-files in v1) — the same
// posture as the webhook/notify credential store (AGENTS.md "已知接线缺口" ②):
// a host-assembly interface with two deployment hooks:
//
//   - SetSQLCredentialResolver: process-level resolver (programmatic wiring);
//   - PLAITA_CREDENTIALS_FILE: plaintext JSON object {"name": {payload}} — a
//     Go-side deployment format, deliberately separate from the Python
//     Fernet store. Enforced with the providers.json discipline (A62-05):
//     mode 0600 or stricter, group/other permission bits fail loud.
//
// Fail-closed: with neither hook configured, every sql_query credential
// lookup names the missing configuration instead of dialing an empty DSN.
// Non-default tenants are refused before the resolver runs (see the tenant
// gate in sql_query.go): the v1 face has no per-tenant stores, so
// tenant-scoped databases need one worker per tenant — the code-node docker
// backend guidance in reverse.
//
// Error text discipline (same red line as the redis node): messages name the
// resolver/file and the missing credential name, never a DSN and never the
// other entries' payloads.

// SQLCredentialResolver resolves a credential NAME into the credential
// payload (the get_credential shape: either {"url": ...} or the
// host/port/user/password/database parts — see sql_dsn.go).
type SQLCredentialResolver func(ctx context.Context, name string) (map[string]any, error)

// sqlCredentialsFileEnv points a Go worker deployment at its plaintext
// credentials file (see the type comment for the format and the Python
// Fernet store caveat).
const sqlCredentialsFileEnv = "PLAITA_CREDENTIALS_FILE"

// sqlCredentialResolverOverride is the process-level hook (same shape as
// redisTargetAllowlistOverride); it wins over the env file.
var sqlCredentialResolverOverride SQLCredentialResolver

// SetSQLCredentialResolver installs the process-level credential resolver
// (deployment hook). Passing nil removes an installed resolver.
func SetSQLCredentialResolver(r SQLCredentialResolver) {
	sqlCredentialResolverOverride = r
}

// sqlResolveCredential resolves a credential name: the process-level hook
// wins, else the env credentials file is read (fresh per lookup — the Python
// store is reread per get_credential too, so console re-exports are picked up
// without a restart).
func sqlResolveCredential(ctx context.Context, name string) (map[string]any, error) {
	if sqlCredentialResolverOverride != nil {
		payload, err := sqlCredentialResolverOverride(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("credential %q: %w", name, err)
		}
		return sqlCredentialPayload(name, payload)
	}

	path := os.Getenv(sqlCredentialsFileEnv)
	if path == "" {
		return nil, fmt.Errorf("credential face is not configured: no resolver installed and %s is unset; "+
			"install SetSQLCredentialResolver or provide a plaintext JSON credentials file "+
			"(the Python console Fernet store is not read by this worker)", sqlCredentialsFileEnv)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("credentials file %s is not readable: %w (a missing file is a deployment "+
			"error here, not an empty store)", path, err)
	}
	if err := checkSQLCredentialsFileMode(path, info.Mode()); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("credentials file %s is not readable: %w", path, err)
	}
	var store map[string]any
	if err := json.Unmarshal(raw, &store); err != nil {
		// Corrupt = fail loud, NOT "no credentials yet" (CredentialStoreCorruptError
		// parity): silently reading an empty store would turn every lookup into
		// "credential not found" and point the investigation at re-creating
		// credentials instead of the broken export.
		return nil, fmt.Errorf("credentials file %s is corrupt: %w", path, err)
	}
	entry, ok := store[name]
	if !ok {
		return nil, fmt.Errorf("credential %q not found in %s (the file holds %d credentials)", name, path, len(store))
	}
	m, _ := entry.(map[string]any)
	return sqlCredentialPayload(name, m)
}

// sqlCredentialPayload rejects an empty resolved payload: a present-but-empty
// credential is a broken entry, and dialing an empty DSN would surface later
// as an incomprehensible driver error.
func sqlCredentialPayload(name string, payload map[string]any) (map[string]any, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("credential %q resolved to an empty payload", name)
	}
	return payload, nil
}

// checkSQLCredentialsFileMode enforces the A62-05 plaintext-secret rule:
// mode 0600 or stricter; any group/other permission bit is a named error.
func checkSQLCredentialsFileMode(path string, mode os.FileMode) error {
	if runtime.GOOS == "windows" {
		// POSIX bits are not the NTFS ACL model; skip like readJSONFile does.
		return nil
	}
	if perm := mode.Perm(); perm&0o077 != 0 {
		return fmt.Errorf("credentials file %s is too permissive (mode %04o); chmod 600 %s", path, perm, path)
	}
	return nil
}

// sqlStringField reads a string credential field; values may arrive as JSON
// numbers (e.g. port) and are stringified the way Python str() would.
func sqlStringField(payload map[string]any, key string) (string, bool) {
	v, ok := payload[key]
	if !ok || v == nil {
		return "", false
	}
	if s, ok := v.(string); ok {
		return s, true
	}
	return strings.TrimSpace(fmt.Sprint(v)), true
}
