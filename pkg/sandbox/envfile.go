package sandbox

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// envNameRe is the POSIX environment-name alphabet. Envfile injection uses a
// shell `export "$line"` loop, so a malformed key would turn into arbitrary
// shell text; reject it at the boundary.
var envNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidateEnv checks the envfile contract: valid names, no newlines or NULs
// in values (the file is one KEY=VALUE per line). Credentials that violate
// this must be rejected, not silently mangled.
func ValidateEnv(env map[string]string) error {
	for key, value := range env {
		if !envNameRe.MatchString(key) {
			return fmt.Errorf("%w: invalid environment variable name %q", ErrConfig, key)
		}
		if strings.ContainsAny(value, "\n\r\x00") {
			return fmt.Errorf("%w: environment variable %q contains a newline or NUL", ErrConfig, key)
		}
	}
	return nil
}

// EnvFileContent renders the envfile (sorted for deterministic tests).
func EnvFileContent(env map[string]string) ([]byte, error) {
	if err := ValidateEnv(env); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(env[key])
		b.WriteByte('\n')
	}
	return []byte(b.String()), nil
}

// WriteEnvFile writes a 0600 envfile. Callers MUST BurnEnvFile in a defer;
// the file never outlives the exec.
func WriteEnvFile(env map[string]string) (string, error) {
	content, err := EnvFileContent(env)
	if err != nil {
		return "", err
	}
	file, err := os.CreateTemp("", "plaita-sandbox-env-*.env")
	if err != nil {
		return "", fmt.Errorf("sandbox envfile: %w", err)
	}
	path := file.Name()
	cleanup := func(err error) (string, error) {
		_ = file.Close()
		_ = os.Remove(path)
		return "", err
	}
	if err := file.Chmod(0o600); err != nil {
		return cleanup(fmt.Errorf("sandbox envfile chmod: %w", err))
	}
	if _, err := file.Write(content); err != nil {
		return cleanup(fmt.Errorf("sandbox envfile write: %w", err))
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("sandbox envfile close: %w", err)
	}
	return path, nil
}

// BurnEnvFile removes the envfile, ignoring cleanup errors (the Python
// burner does the same — it runs in finally blocks).
func BurnEnvFile(path string) {
	if path == "" {
		return
	}
	_ = os.Remove(path)
}

// Redactor replaces injected env values with [REDACTED:NAME]. Values shorter
// than six characters are skipped to avoid catastrophic substring matches.
// Longer values are replaced first so a short value cannot eat a long value's
// prefix.
type Redactor struct {
	pairs []redactPair
}

type redactPair struct {
	value string
	name  string
}

// NewRedactor builds a redactor over all injected values.
func NewRedactor(env map[string]string) *Redactor {
	pairs := make([]redactPair, 0, len(env))
	for name, value := range env {
		if len(value) >= 6 {
			pairs = append(pairs, redactPair{value: value, name: name})
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return len(pairs[i].value) > len(pairs[j].value) })
	return &Redactor{pairs: pairs}
}

// Redact replaces every occurrence of an injected secret.
func (r *Redactor) Redact(text string) string {
	if r == nil || text == "" {
		return text
	}
	for _, pair := range r.pairs {
		text = strings.ReplaceAll(text, pair.value, "[REDACTED:"+pair.name+"]")
	}
	return text
}
