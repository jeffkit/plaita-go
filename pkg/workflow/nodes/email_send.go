package nodes

// EmailSendNode — 邮件发送连接器（plaita-nodes email.py 薄壳节点；SMTP
// 协议适配在 notify_backends._backend_email_send）。凭据数据::
//
//	{"host": "smtp.example.com", "port": 465,
//	 "username": "...", "password": "...", "use_ssl": true, "use_tls": false}
//
// body_is_html 时按 multipart/alternative 发送：去标签纯文本在前作降级
// 备选，HTML 在后为主件（#12）。net/smtp 的发送口经 notifySMTPSender 注入
// （测试不发真信）。

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"mime/quotedprintable"
	"net"
	"net/smtp"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeEmailSend is the email_send node type.
const nodeTypeEmailSend types.NodeType = "email_send"

func init() {
	workflow.RegisterNodeType(
		nodeTypeEmailSend,
		func(ctx context.Context) workflow.Node { return new(EmailSend) },
	)
}

// EmailSend sends an email over SMTP; credentials carry the server config.
type EmailSend struct {
	BaseNode
	Credential string `json:"credential,omitempty"`
	// To 收件人：字符串、逗号分隔或列表（支持表达式）。
	To any `json:"to,omitempty"`
	// Subject 主题（支持表达式）。
	Subject any `json:"subject,omitempty"`
	// Body 正文（支持表达式）。
	Body any `json:"body,omitempty"`
	// BodyIsHTML 正文按 HTML 发送（multipart/alternative，纯文本降级备选）。
	BodyIsHTML bool `json:"body_is_html,omitempty"`
	DryRun     bool `json:"dry_run,omitempty"`
	// Credentials resolves the credential name; wired by the host runtime.
	Credentials NotifyCredentialStore `json:"-"`

	// smtpSender performs delivery; nil → real net/smtp path. Test hook.
	smtpSender notifySMTPSender `json:"-"`
}

// GetType returns the email_send node type.
func (e *EmailSend) GetType() workflow.NodeType { return nodeTypeEmailSend }

// Execute mirrors email.py EmailSendNode.execute: recipients are evaluated
// BEFORE the dry-run gate (the dry output carries them), then the body is
// evaluated and the email backend does credential resolution + SMTP.
func (e *EmailSend) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	// dry-run 最先判（收件人仍求值，dry 输出可检查）：不解析凭据、不连 SMTP。
	recipients, err := notifyEvalRecipients(ctx, e.To)
	if err != nil {
		return nil, fmt.Errorf("email_send %q: %w", e.ID, err)
	}
	if e.DryRun || notifyGlobalDryRun(ctx) {
		return webhookResult(e, map[string]any{
			"status": "dry-run", "to": recipients, "dry_run": true,
		}), nil
	}
	body := ""
	if e.Body != nil {
		v, err := expression.Eval(ctx, e.Body)
		if err != nil {
			return nil, fmt.Errorf("email_send %q: %w", e.ID, err)
		}
		body = notifyTextOf(v)
	}
	out, err := notifyEmailBackend(ctx, e.Credentials, e.Credential, e.To, e.Subject, e.BodyIsHTML, body, e.smtpSender)
	if err != nil {
		return nil, webhookWrapErr(nodeTypeEmailSend, e.ID, err)
	}
	return webhookResult(e, out), nil
}

// notifySMTPConfig is the parsed SMTP credential blob.
type notifySMTPConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	UseSSL   bool
	UseTLS   bool
}

// notifySMTPSender performs the actual delivery (envelope + full RFC5322
// message); injectable so tests never open a real connection.
type notifySMTPSender func(ctx context.Context, cfg *notifySMTPConfig, from string, to []string, msg []byte) error

// notifyEvalRecipients mirrors the to-field handling shared by email.py and
// _backend_email_send: `evaluate(to) or []`, comma-split when a string.
func notifyEvalRecipients(ctx context.Context, raw any) ([]string, error) {
	v, err := expression.Eval(ctx, raw)
	if err != nil {
		return nil, err
	}
	recipients := []string{}
	if !notifyIsTruthy(v) {
		return recipients, nil
	}
	if s, ok := v.(string); ok {
		for _, part := range strings.Split(s, ",") {
			if part = strings.TrimSpace(part); part != "" {
				recipients = append(recipients, part)
			}
		}
		return recipients, nil
	}
	if list, ok := v.([]any); ok {
		for _, item := range list {
			recipients = append(recipients, notifyStringOf(item))
		}
		return recipients, nil
	}
	return append(recipients, notifyStringOf(v)), nil
}

// notifyEmailBackend mirrors _backend_email_send: credential first (missing
// credential/host are deterministic config errors), then recipient/subject
// evaluation, message assembly, SMTP delivery. Output: {"status": "sent",
// "to": recipients}.
func notifyEmailBackend(ctx context.Context, store NotifyCredentialStore, credName string, rawTo, rawSubject any, bodyIsHTML bool, text string, sender notifySMTPSender) (map[string]any, error) {
	cred, err := notifyCredentialData(ctx, store, credName)
	if err != nil {
		return nil, err
	}
	cfg, err := notifySMTPConfigFromCred(cred, credName)
	if err != nil {
		return nil, err
	}
	recipients, err := notifyEvalRecipients(ctx, rawTo)
	if err != nil {
		return nil, err
	}
	subject := ""
	if rawSubject != nil {
		v, err := expression.Eval(ctx, rawSubject)
		if err != nil {
			return nil, err
		}
		subject = notifyTextOf(v)
	}

	from := cfg.Username
	if from == "" {
		from = "plaita"
	}
	msg, err := notifyBuildEmailMessage(from, recipients, subject, text, bodyIsHTML)
	if err != nil {
		return nil, err
	}
	if sender == nil {
		sender = notifySMTPSendReal
	}
	if err := sender(ctx, cfg, from, recipients, msg); err != nil {
		return nil, err
	}
	return map[string]any{"status": "sent", "to": recipients}, nil
}

// notifySMTPConfigFromCred mirrors the host/port parsing in
// _backend_email_send: host required; port defaults to 465 with use_ssl,
// else 587.
func notifySMTPConfigFromCred(cred map[string]any, credName string) (*notifySMTPConfig, error) {
	cfg := &notifySMTPConfig{
		Host:     notifyCredentialString(cred, "host"),
		Username: notifyCredentialString(cred, "username"),
		Password: notifyCredentialString(cred, "password"),
		UseSSL:   notifyIsTruthy(cred["use_ssl"]),
		UseTLS:   notifyIsTruthy(cred["use_tls"]),
	}
	if cfg.Host == "" {
		return nil, &NotifyDeterministicError{Msg: fmt.Sprintf("凭据 %q 缺少 host 字段", credName)}
	}
	cfg.Port = 587
	if cfg.UseSSL {
		cfg.Port = 465
	}
	if raw, exists := cred["port"]; exists && raw != nil {
		p, ok := notifyAsPythonInt(raw)
		if !ok {
			return nil, &NotifyDeterministicError{Msg: fmt.Sprintf("凭据 %q 的 port 不是合法整数: %s", credName, notifyStringOf(raw))}
		}
		cfg.Port = int(p)
	}
	return cfg, nil
}

// ── message assembly (notify_backends._build_email_message) ─────────────

// notifyBuildEmailMessage renders the RFC5322 message: From/To/Subject
// headers plus a text/plain body; with bodyIsHTML a multipart/alternative
// whose plain part is the tag-stripped readable fallback (#12). Lines use
// CRLF; non-ASCII bodies travel quoted-printable, non-ASCII headers get
// RFC2047 B-encoding. Header values carrying a line boundary are a named
// rejection before any wire bytes exist (#71) — Python's EmailMessage
// raises ValueError on the same assignment (email/policy.py
// header_store_parse); notifyEncodeHeaderText alone cannot be the guard
// because it is an identity function for pure-ASCII values.
func notifyBuildEmailMessage(from string, recipients []string, subject, text string, bodyIsHTML bool) ([]byte, error) {
	for _, h := range []struct{ name, value string }{
		{"From", from},
		{"To", strings.Join(recipients, ", ")},
		{"Subject", subject},
	} {
		if notifyHeaderHasLineBreak(h.value) {
			return nil, &NotifyDeterministicError{Msg: fmt.Sprintf(
				"%s 头值包含 CR/LF 等换行控制字符，拒绝注入邮件头（Header values may not contain linefeed or carriage return characters）", h.name)}
		}
	}
	var b bytes.Buffer
	line := func(s string) {
		b.WriteString(s)
		b.WriteString("\r\n")
	}
	line("From: " + notifyEncodeHeaderText(from))
	line("To: " + notifyEncodeHeaderText(strings.Join(recipients, ", ")))
	line("Subject: " + notifyEncodeHeaderText(subject))
	line("MIME-Version: 1.0")

	if bodyIsHTML {
		plain := notifyHTMLToText(text)
		if plain == "" {
			plain = "（正文为 HTML，当前客户端不支持显示）"
		}
		boundary := notifyBoundary()
		line(fmt.Sprintf("Content-Type: multipart/alternative; boundary=%q", boundary))
		line("")
		line("--" + boundary)
		notifyWriteMIMEBody(&b, "text/plain", plain)
		line("--" + boundary)
		notifyWriteMIMEBody(&b, "text/html", text)
		line("--" + boundary + "--")
	} else {
		line(`Content-Type: text/plain; charset="utf-8"`)
		line("Content-Transfer-Encoding: quoted-printable")
		line("")
		b.WriteString(notifyQuotedPrintable(text))
		b.WriteString("\r\n")
	}
	return b.Bytes(), nil
}

// notifyHeaderHasLineBreak reports whether v contains any character Python's
// str.splitlines treats as a line boundary — the exact set CPython's
// header_store_parse rejects (CR/LF plus VT/FF/line/page separators and the
// Unicode NEL/LS/PS). A tab is not a boundary and stays legal.
func notifyHeaderHasLineBreak(v string) bool {
	for _, r := range v {
		switch r {
		case '\n', '\r', '\v', '\f', 0x1c, 0x1d, 0x1e, 0x85, 0x2028, 0x2029:
			return true
		}
	}
	return false
}

// notifyWriteMIMEBody writes one alternative part (headers + QP body).
func notifyWriteMIMEBody(b *bytes.Buffer, contentType, text string) {
	b.WriteString(fmt.Sprintf("Content-Type: %s; charset=\"utf-8\"\r\n", contentType))
	b.WriteString("Content-Transfer-Encoding: quoted-printable\r\n\r\n")
	b.WriteString(notifyQuotedPrintable(text))
	b.WriteString("\r\n")
}

// notifyQuotedPrintable encodes a body with CRLF line endings (Python's
// EmailMessage + smtplib land the wire in the same shape).
func notifyQuotedPrintable(text string) string {
	normalized := strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\n", "\r\n")
	var buf bytes.Buffer
	w := quotedprintable.NewWriter(&buf)
	_, _ = w.Write([]byte(normalized))
	_ = w.Close()
	return buf.String()
}

// notifyEncodeHeaderText applies RFC2047 B-encoding when the header value
// carries non-ASCII (Python's EmailMessage encodes automatically).
func notifyEncodeHeaderText(v string) string {
	for _, r := range v {
		if r > 127 {
			return "=?utf-8?b?" + base64.StdEncoding.EncodeToString([]byte(v)) + "?="
		}
	}
	return v
}

// ── HTML → text fallback (notify_backends._HtmlToText) ──────────────────

var (
	notifyHTMLBlockTags = map[string]bool{
		"p": true, "div": true, "br": true, "li": true, "tr": true,
		"td": true, "th": true, "h1": true, "h2": true, "h3": true,
		"h4": true, "h5": true, "h6": true, "blockquote": true,
		"pre": true, "table": true, "ul": true, "ol": true, "hr": true,
	}
	notifyHTMLSkipTags = map[string]bool{
		"script": true, "style": true, "head": true, "title": true,
	}
	notifyEntityRe = regexp.MustCompile(`&(#[xX][0-9a-fA-F]+|#[0-9]+|[a-zA-Z][a-zA-Z0-9]*);`)
)

// notifyHTMLToText strips HTML down to readable plain text: block tags
// become newlines, script/style/head/title content is skipped, entities are
// decoded, and the result is whitespace-collapsed per line (empty lines
// dropped). Covers the common entity set — enough for the plain-text
// fallback part, not a full HTML parser.
func notifyHTMLToText(html string) string {
	var b strings.Builder
	skipDepth := 0
	i := 0
	for i < len(html) {
		if html[i] != '<' {
			next := strings.IndexByte(html[i:], '<')
			var chunk string
			if next < 0 {
				chunk = html[i:]
				i = len(html)
			} else {
				chunk = html[i : i+next]
				i += next
			}
			if skipDepth == 0 {
				b.WriteString(notifyDecodeEntities(chunk))
			}
			continue
		}
		rest := html[i:]
		if strings.HasPrefix(rest, "<!--") { // comments produce no output
			end := strings.Index(rest, "-->")
			if end < 0 {
				break
			}
			i += end + 3
			continue
		}
		if strings.HasPrefix(rest, "<!") { // doctype/declarations ignored
			end := strings.IndexByte(rest, '>')
			if end < 0 {
				break
			}
			i += end + 1
			continue
		}
		end := strings.IndexByte(rest, '>')
		if end < 0 {
			break // unterminated tag at EOF: nothing more to render
		}
		tagBody := rest[1:end]
		i += end + 1
		name, selfClosing := notifyParseTagBody(tagBody)
		lower := strings.ToLower(name)
		if lower == "" {
			continue
		}
		if notifyHTMLSkipTags[lower] {
			switch {
			case selfClosing:
				// start+end cancel out
			case strings.HasPrefix(tagBody, "/"):
				if skipDepth > 0 {
					skipDepth--
				}
			default:
				skipDepth++
			}
			continue
		}
		if notifyHTMLBlockTags[lower] {
			b.WriteString("\n")
		}
	}
	lines := strings.Split(b.String(), "\n")
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		if collapsed := strings.Join(strings.Fields(l), " "); collapsed != "" {
			out = append(out, collapsed)
		}
	}
	return strings.Join(out, "\n")
}

// notifyParseTagBody extracts the tag name from the inside of `<>` and
// reports self-closing (`<br/>`).
func notifyParseTagBody(body string) (name string, selfClosing bool) {
	body = strings.TrimSpace(body)
	if strings.HasPrefix(body, "/") {
		return strings.TrimPrefix(body, "/"), false
	}
	if strings.HasSuffix(body, "/") {
		selfClosing = true
		body = strings.TrimSuffix(body, "/")
	}
	for _, f := range strings.Fields(body) {
		return f, selfClosing
	}
	return "", selfClosing
}

// notifyDecodeEntities decodes the common named entities plus numeric
// references (HTMLParser convert_charrefs equivalent for the common set).
func notifyDecodeEntities(s string) string {
	if !strings.Contains(s, "&") {
		return s
	}
	return notifyEntityRe.ReplaceAllStringFunc(s, func(m string) string {
		name := m[1 : len(m)-1]
		switch name {
		case "amp":
			return "&"
		case "lt":
			return "<"
		case "gt":
			return ">"
		case "quot":
			return "\""
		case "apos":
			return "'"
		case "nbsp":
			return "\u00a0"
		}
		if strings.HasPrefix(name, "#") {
			digits := name[1:]
			base := 10
			if digits[0] == 'x' || digits[0] == 'X' {
				digits = digits[1:]
				base = 16
			}
			if code, err := strconv.ParseUint(digits, base, 32); err == nil {
				return string(rune(code))
			}
		}
		return m
	})
}

// ── real SMTP delivery (net/smtp; mirrors smtplib usage) ────────────────

// notifyTLSConfig mirrors smtplib's ssl._create_stdlib_context: ServerName
// set (SNI), certificate verification disabled — Python's smtplib never
// verified SMTP server certificates, and corporate relays run self-signed
// certs; porting a stricter check would break the same setups Python serves.
func notifyTLSConfig(host string) *tls.Config {
	return &tls.Config{ServerName: host, InsecureSkipVerify: true}
}

// notifySMTPSendReal mirrors the smtplib block in _backend_email_send:
// implicit TLS when use_ssl, otherwise STARTTLS when use_tls; login when
// username+password are set; envelope from = From header, rcpt = To list.
// A 20s budget covers dial and session (Python timeout=20).
//
// Known stdlib divergence: smtp.PlainAuth refuses to send credentials over
// an unencrypted connection to a non-localhost server (smtplib has no such
// guard) — plain-no-TLS relays need an authenticated-smtp extension here.
func notifySMTPSendReal(ctx context.Context, cfg *notifySMTPConfig, from string, to []string, msg []byte) error {
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	dialer := &net.Dialer{Timeout: 20 * time.Second}
	var conn net.Conn
	var err error
	if cfg.UseSSL {
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, notifyTLSConfig(cfg.Host))
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))

	client, err := smtp.NewClient(conn, cfg.Host)
	if err != nil {
		return err
	}
	defer client.Close()

	if !cfg.UseSSL && cfg.UseTLS {
		if err := client.StartTLS(notifyTLSConfig(cfg.Host)); err != nil {
			return err
		}
	}
	if cfg.Username != "" && cfg.Password != "" {
		if err := client.Auth(smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.Host)); err != nil {
			return err
		}
	}
	if err := client.Mail(from); err != nil {
		return err
	}
	for _, rcpt := range to {
		if err := client.Rcpt(rcpt); err != nil {
			return err
		}
	}
	w, err := client.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(msg); err != nil {
		_ = w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return client.Quit()
}
