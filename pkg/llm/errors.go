package llm

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// DeterministicError ports plaita-nodes _retry.DeterministicError: endpoint
// configuration problems, malformed payloads and non-retryable HTTP 4xx. The
// engine must not burn retryTimes on it (plaita classifies via the ValueError
// mixin, A2R7-04).
type DeterministicError struct {
	err error
}

func detErr(format string, args ...any) *DeterministicError {
	return &DeterministicError{err: fmt.Errorf(format, args...)}
}

// Error renders the message.
func (e *DeterministicError) Error() string { return e.err.Error() }

// Unwrap exposes the cause.
func (e *DeterministicError) Unwrap() error { return e.err }

// APIError is a non-2xx response from a completions endpoint. Body is the
// (truncated, scrubbed) response text; Error() composes the plaita-nodes
// message shape "LLM 请求失败 {status}: {body[:300]}" (llm.py:153-154).
type APIError struct {
	Status int
	Body   string
}

// Error renders the classified message.
func (e *APIError) Error() string {
	return fmt.Sprintf("LLM 请求失败 %d: %s", e.Status, e.Body)
}

// Retryable mirrors is_retryable_http_status (_retry.py:50): 5xx and
// 408/425/429 are transient; every other 4xx is a deterministic request
// error (401/403/404 …) that a retry cannot fix.
func (e *APIError) Retryable() bool { return IsRetryableStatus(e.Status) }

// retryableHTTPStatus ports RETRYABLE_HTTP_STATUS (_retry.py:29).
var retryableHTTPStatus = map[int]bool{408: true, 425: true, 429: true}

// IsRetryableStatus ports is_retryable_http_status (_retry.py:50-52).
func IsRetryableStatus(status int) bool {
	return status >= 500 || retryableHTTPStatus[status]
}

// Retryable classifies an error for the engine's retry gate:
//
//   - *DeterministicError (or anything wrapping one) → never retry;
//   - *APIError → per the HTTP status table;
//   - anything else (transport failures, timeouts, malformed payloads) →
//     retryable, mirroring Python where they surface as plain RuntimeError /
//     RequestException and the engine retries them (A2R7-04).
func Retryable(err error) bool {
	for err != nil {
		var tooLarge *ResponseTooLargeError
		if errors.As(err, &tooLarge) {
			return false
		}
		var det *DeterministicError
		if errors.As(err, &det) {
			return false
		}
		var api *APIError
		if errors.As(err, &api) {
			return api.Retryable()
		}
		err = errors.Unwrap(err)
	}
	return true
}

// --- log/error text safety (minimal port of plaita-nodes _redact.py) ---

const (
	unprintable = "<unprintable>"
	placeholder = "***"
)

var (
	absURLRe = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.\-]{0,31}://[^ \t\r\n\f\v]+`)
	// credentialRunRe masks password/token/api_key style runs in free text.
	credentialRunRe = regexp.MustCompile(`(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|client[_-]?secret|private[_-]?key)\b(\s*[=:]\s*)("[^"]*"|'[^']*'|[^\s&,;]+)`)
)

// scrubText masks URLs (absolute URLs keep only scheme://host[:port], every
// path segment and the whole query/fragment become ***) and bare credential
// runs. Total: never fails, never echoes the raw secret-bearing text
// (plaita-nodes _redact._scrub_url_text discipline).
func scrubText(text string) string {
	out := absURLRe.ReplaceAllStringFunc(text, renderURL)
	out = credentialRunRe.ReplaceAllString(out, "$1$2"+placeholder)
	return out
}

// ScrubText masks URLs and bare credential runs in arbitrary error/log text
// (the plaita-nodes _redact.py discipline, exported for endpoint-adjacent
// code paths such as the decision node's jev protocol).
func ScrubText(text string) string { return scrubText(text) }

// renderURL masks one absolute URL down to scheme://host[:port].
func renderURL(token string) string {
	parts, err := url.Parse(token)
	if err != nil {
		return unprintable
	}
	scheme := strings.ToLower(parts.Scheme)
	host := parts.Hostname()
	if (scheme != "http" && scheme != "https") || host == "" {
		return unprintable
	}
	var b strings.Builder
	b.WriteString(scheme)
	b.WriteString("://")
	b.WriteString(host)
	if p := parts.Port(); p != "" {
		b.WriteString(":")
		b.WriteString(p)
	}
	for _, seg := range strings.Split(parts.Path, "/") {
		b.WriteString("/")
		if seg != "" {
			b.WriteString(placeholder)
		}
	}
	if parts.RawQuery != "" {
		b.WriteString("?")
		b.WriteString(placeholder)
	}
	if parts.Fragment != "" {
		b.WriteString("#")
		b.WriteString(placeholder)
	}
	return b.String()
}

// truncateRunes slices s to at most n runes (Python slices str by
// characters, not bytes).
func truncateRunes(s string, n int) string {
	if n <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
