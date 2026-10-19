package nodes

// Tests for email_send. No real mail ever leaves the process: delivery goes
// through the injected notifySMTPSender or a local fake SMTP server (plain
// and in-memory-TLS variants). Semantic authority: plaita-nodes email.py +
// notify_backends (_backend_email_send, _build_email_message, _HtmlToText);
// cases mirror tests/test_connectors.py's email_send section.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/textproto"
	"strconv"
	"strings"
	"testing"
	"time"
)

// notifyDecodeTestMessage parses a built RFC5322 message into headers and
// the decoded part bodies (plain / html by content type).
func notifyDecodeTestMessage(t *testing.T, msg []byte) (header mail.Header, plain, html string) {
	t.Helper()
	parsed, err := mail.ReadMessage(bytes.NewReader(msg))
	if err != nil {
		t.Fatalf("mail.ReadMessage: %v", err)
	}
	header = parsed.Header
	readPart := func(r io.Reader, contentType string) {
		body := new(strings.Builder)
		_, _ = io.Copy(body, r)
		mt, _, err := mime.ParseMediaType(contentType)
		if err != nil {
			return
		}
		switch mt {
		case "text/plain":
			plain = body.String()
		case "text/html":
			html = body.String()
		}
	}
	mediaType, params, err := mime.ParseMediaType(header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("content type: %v", err)
	}
	if strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(parsed.Body, params["boundary"])
		for {
			part, err := mr.NextPart()
			if err != nil {
				break
			}
			var r io.Reader = part
			if part.Header.Get("Content-Transfer-Encoding") == "quoted-printable" {
				r = quotedprintable.NewReader(part)
			}
			readPart(r, part.Header.Get("Content-Type"))
		}
	} else {
		var r io.Reader = parsed.Body
		if header.Get("Content-Transfer-Encoding") == "quoted-printable" {
			r = quotedprintable.NewReader(parsed.Body)
		}
		readPart(r, mediaType)
	}
	return header, plain, html
}

// notifyHeader reads a header the way Python's headerregistry does:
// RFC2047 encoded-words decoded on access (net/mail returns the raw value).
func notifyHeader(t *testing.T, h mail.Header, key string) string {
	t.Helper()
	v := h.Get(key)
	if decoded, err := (&mime.WordDecoder{}).DecodeHeader(v); err == nil {
		return decoded
	}
	return v
}

func TestEmailSendEnvelopeAndOutput(t *testing.T) {
	var got struct {
		cfg  *notifySMTPConfig
		from string
		to   []string
		msg  []byte
	}
	called := false
	sender := func(_ context.Context, cfg *notifySMTPConfig, from string, to []string, msg []byte) error {
		called = true
		got.cfg, got.from, got.to, got.msg = cfg, from, to, msg
		return nil
	}
	node := &EmailSend{
		BaseNode: BaseNode{ID: "n"}, Credential: "smtp",
		To: []any{"a@x.com", "b@x.com"}, Subject: "告警", Body: "磁盘 90%",
		smtpSender: sender,
		Credentials: notifyTestCredentialStore{"smtp": {
			"host": "smtp.test", "port": 587, "username": "bot@test",
			"password": "pw", "use_tls": true,
		}},
	}
	res, err := node.Execute(notifyTestContext(t, nil, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !called {
		t.Fatal("sender not called")
	}
	out := res.Result.(map[string]any)
	if out["status"] != "sent" {
		t.Errorf("status = %v", out["status"])
	}
	to, _ := out["to"].([]string)
	if len(to) != 2 || to[0] != "a@x.com" || to[1] != "b@x.com" {
		t.Errorf("to = %v", out["to"])
	}
	if got.cfg.Host != "smtp.test" || got.cfg.Port != 587 || !got.cfg.UseTLS || got.cfg.UseSSL {
		t.Errorf("cfg = %#v", got.cfg)
	}
	if got.from != "bot@test" {
		t.Errorf("from = %q", got.from)
	}
	header, plain, html := notifyDecodeTestMessage(t, got.msg)
	if header.Get("To") != "a@x.com, b@x.com" {
		t.Errorf("To = %q", header.Get("To"))
	}
	if subj := notifyHeader(t, header, "Subject"); subj != "告警" {
		t.Errorf("Subject = %q", subj)
	}
	if header.Get("From") != "bot@test" {
		t.Errorf("From = %q", header.Get("From"))
	}
	if strings.TrimSpace(plain) != "磁盘 90%" || html != "" {
		t.Errorf("plain = %q html = %q", plain, html)
	}
}

func TestEmailSendRecipientFormsAndDefaults(t *testing.T) {
	cases := []struct {
		name  string
		raw   any
		input map[string]any
		want  []string
	}{
		{"nil_to_empty", nil, nil, []string{}},
		{"comma_string", " a@x.com ,b@x.com ,", nil, []string{"a@x.com", "b@x.com"}},
		{"list", []any{"a@x.com", "b@x.com"}, nil, []string{"a@x.com", "b@x.com"}},
		{"expression", "$INPUT.rcpts", map[string]any{"rcpts": []any{"r1@x.com", "r2@x.com"}}, []string{"r1@x.com", "r2@x.com"}},
		{"empty_string_falsy", "", nil, []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var to []string
			sender := func(_ context.Context, _ *notifySMTPConfig, _ string, rcpts []string, _ []byte) error {
				to = rcpts
				return nil
			}
			node := &EmailSend{
				BaseNode: BaseNode{ID: "n"}, Credential: "smtp",
				To: tc.raw, smtpSender: sender,
				Credentials: notifyTestCredentialStore{"smtp": {"host": "smtp.test"}},
			}
			if _, err := node.Execute(notifyTestContext(t, tc.input, nil)); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if len(to) != len(tc.want) {
				t.Fatalf("to = %v, want %v", to, tc.want)
			}
			for i := range tc.want {
				if to[i] != tc.want[i] {
					t.Fatalf("to = %v, want %v", to, tc.want)
				}
			}
		})
	}
}

func TestEmailSendFromDefaultsToPlaita(t *testing.T) {
	var from string
	sender := func(_ context.Context, _ *notifySMTPConfig, f string, _ []string, _ []byte) error {
		from = f
		return nil
	}
	node := &EmailSend{BaseNode: BaseNode{ID: "n"}, Credential: "smtp", To: "a@x.com",
		smtpSender:  sender,
		Credentials: notifyTestCredentialStore{"smtp": {"host": "smtp.test"}}}
	if _, err := node.Execute(notifyTestContext(t, nil, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if from != "plaita" {
		t.Errorf("from = %q, want plaita default", from)
	}
}

func TestEmailSendPortDefaults(t *testing.T) {
	cases := []struct {
		name string
		cred map[string]any
		want int
	}{
		{"plain_default_587", map[string]any{"host": "h"}, 587},
		{"ssl_default_465", map[string]any{"host": "h", "use_ssl": true}, 465},
		{"explicit_override", map[string]any{"host": "h", "use_ssl": true, "port": 2465}, 2465},
		{"numeric_string", map[string]any{"host": "h", "port": "2587"}, 2587},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := notifySMTPConfigFromCred(tc.cred, "c")
			if err != nil {
				t.Fatalf("config: %v", err)
			}
			if cfg.Port != tc.want {
				t.Errorf("port = %d, want %d", cfg.Port, tc.want)
			}
		})
	}
	badPort := map[string]any{"host": "h", "port": "smtp"}
	if _, err := notifySMTPConfigFromCred(badPort, "c"); err == nil {
		t.Error("non-numeric port must fail")
	}
	if _, err := notifySMTPConfigFromCred(map[string]any{}, "c"); err == nil ||
		!strings.Contains(err.Error(), "缺少 host 字段") {
		t.Errorf("missing host err = %v", err)
	}
}

func TestEmailSendDryRunEvaluatesRecipientsButNoSend(t *testing.T) {
	sent := false
	sender := func(_ context.Context, _ *notifySMTPConfig, _ string, _ []string, _ []byte) error {
		sent = true
		return nil
	}
	store := notifyTestCredentialStore{"smtp": {"host": "smtp.test"}}

	node := &EmailSend{BaseNode: BaseNode{ID: "n"}, Credential: "smtp",
		To: "$INPUT.rcpts", smtpSender: sender, DryRun: true, Credentials: store}
	res, err := node.Execute(notifyTestContext(t, map[string]any{"rcpts": "a@x.com"}, nil))
	if err != nil {
		t.Fatalf("dry Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["status"] != "dry-run" || out["dry_run"] != true {
		t.Errorf("dry result = %#v", out)
	}
	if to, _ := out["to"].([]string); len(to) != 1 || to[0] != "a@x.com" {
		t.Errorf("dry to = %v", out["to"])
	}

	global := &EmailSend{BaseNode: BaseNode{ID: "n"}, Credential: "smtp",
		To: "b@x.com", smtpSender: sender, Credentials: store}
	res, err = global.Execute(notifyTestContext(t, nil, map[string]any{"dry_run": true}))
	if err != nil {
		t.Fatalf("global dry Execute: %v", err)
	}
	if res.Result.(map[string]any)["dry_run"] != true {
		t.Errorf("global dry result = %#v", res.Result)
	}
	if sent {
		t.Error("sender called during dry-run")
	}
}

func TestEmailSendCredentialFailures(t *testing.T) {
	node := &EmailSend{BaseNode: BaseNode{ID: "n"}, To: "a@x.com"}
	if _, err := node.Execute(notifyTestContext(t, nil, nil)); err == nil ||
		!strings.Contains(err.Error(), "缺少 credential 字段") {
		t.Errorf("missing credential err = %v", err)
	}
	noHost := &EmailSend{BaseNode: BaseNode{ID: "n"}, Credential: "smtp", To: "a@x.com",
		Credentials: notifyTestCredentialStore{"smtp": {"port": 587}}}
	_, err := noHost.Execute(notifyTestContext(t, nil, nil))
	if err == nil || !strings.Contains(err.Error(), "缺少 host 字段") {
		t.Errorf("missing host err = %v", err)
	}
	var det *NotifyDeterministicError
	if !errors.As(err, &det) {
		t.Errorf("config error %T not deterministic", err)
	}
}

func TestEmailSendHTMLBodyMultipartAlternative(t *testing.T) {
	html := ("<html><head><title>标题</title></head><body>" +
		"<h1>磁盘告警</h1><p>使用率 <b>90%</b> &amp; 持续上升<br>请处理</p>" +
		"<script>alert(1)</script></body></html>")
	var msg []byte
	sender := func(_ context.Context, _ *notifySMTPConfig, _ string, _ []string, m []byte) error {
		msg = m
		return nil
	}
	node := &EmailSend{BaseNode: BaseNode{ID: "n"}, Credential: "smtp",
		To: []any{"a@x.com"}, Subject: "告警", Body: html, BodyIsHTML: true,
		smtpSender:  sender,
		Credentials: notifyTestCredentialStore{"smtp": {"host": "smtp.test", "username": "bot@test"}}}
	if _, err := node.Execute(notifyTestContext(t, nil, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(string(msg), `Content-Type: multipart/alternative; boundary="`) {
		t.Fatalf("missing multipart header:\n%s", msg)
	}
	header, plain, htmlPart := notifyDecodeTestMessage(t, msg)
	if subj := notifyHeader(t, header, "Subject"); subj != "告警" {
		t.Errorf("Subject = %q", subj)
	}
	if plain == "" || htmlPart == "" {
		t.Fatalf("alternative parts missing: plain=%q html=%q", plain, htmlPart)
	}
	if strings.ContainsAny(plain, "<>") {
		t.Errorf("plain fallback keeps markup: %q", plain)
	}
	for _, want := range []string{"磁盘告警", "使用率 90% & 持续上升", "请处理"} {
		if !strings.Contains(plain, want) {
			t.Errorf("plain %q missing %q", plain, want)
		}
	}
	if strings.Contains(plain, "alert") || strings.Contains(plain, "标题") {
		t.Errorf("plain leaks skipped content: %q", plain)
	}
	if strings.TrimSpace(htmlPart) != strings.TrimSpace(html) {
		t.Errorf("html part = %q, want original", htmlPart)
	}
}

func TestEmailSendHTMLOnlyMarkupGetsPlaceholder(t *testing.T) {
	var msg []byte
	sender := func(_ context.Context, _ *notifySMTPConfig, _ string, _ []string, m []byte) error {
		msg = m
		return nil
	}
	node := &EmailSend{BaseNode: BaseNode{ID: "n"}, Credential: "smtp",
		To: "a@x.com", Body: "<img src='https://x/y.png'>", BodyIsHTML: true,
		smtpSender:  sender,
		Credentials: notifyTestCredentialStore{"smtp": {"host": "smtp.test"}}}
	if _, err := node.Execute(notifyTestContext(t, nil, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	_, plain, _ := notifyDecodeTestMessage(t, msg)
	if strings.ContainsAny(plain, "<>") || !strings.Contains(plain, "（正文为 HTML，当前客户端不支持显示）") {
		t.Errorf("plain = %q", plain)
	}
}

func TestNotifyHTMLToText(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"plain_passes_through", "no markup", "no markup"},
		{"entities", "a &lt; b &gt; c &quot;d&quot; &amp; &#65;&#x42; e&nbsp;f", "a < b > c \"d\" & AB e f"},
		{"comment_dropped", "x<!-- hidden -->y", "xy"},
		{"doctype_dropped", "<!DOCTYPE html><p>hi</p>", "hi"},
		{"whitespace_collapsed", "<p>  a\t b </p>  <div>c</div>", "a b\nc"},
		{"embedded_newline_starts_line", "<p>  a\t\n b </p>  <div>c</div>", "a\nb\nc"},
		{"skip_tags_content_dropped", "<style>p{}</style>keep<script>bad()</script>", "keep"},
		{"self_closing_br", "line1<br/>line2", "line1\nline2"},
		{"nested_blocks", "<table><tr><td>c1</td><td>c2</td></tr></table>", "c1\nc2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := notifyHTMLToText(tc.in); got != tc.want {
				t.Errorf("notifyHTMLToText(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNotifyEncodeHeaderText(t *testing.T) {
	if ascii := notifyEncodeHeaderText("daily report"); ascii != "daily report" {
		t.Errorf("ascii header = %q", ascii)
	}
	encoded := notifyEncodeHeaderText("告警")
	if !strings.HasPrefix(encoded, "=?utf-8?b?") || !strings.HasSuffix(encoded, "?=") {
		t.Fatalf("encoded = %q", encoded)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSuffix(strings.TrimPrefix(encoded, "=?utf-8?b?"), "?="))
	if err != nil || string(raw) != "告警" {
		t.Errorf("encoded roundtrip = %q (%v)", string(raw), err)
	}
}

// ── local fake SMTP server for the real net/smtp path ───────────────────

// notifyTestSMTPServer is a minimal SMTP dialogue on a local port that
// records the envelope and message. Supports an optional TLS wrapper.
type notifyTestSMTPServer struct {
	listener net.Listener
	MailFrom string
	RcptTo   []string
	Data     []byte
	Authed   string
}

// notifyStartFakeSMTP serves a plain SMTP dialogue on a local port.
func notifyStartFakeSMTP(t *testing.T) *notifyTestSMTPServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &notifyTestSMTPServer{listener: ln}
	go srv.serve(ln)
	t.Cleanup(func() { _ = ln.Close() })
	return srv
}

// notifyStartFakeSMTPS serves the same dialogue over an in-memory TLS cert.
func notifyStartFakeSMTPS(t *testing.T, cert tls.Certificate) *notifyTestSMTPServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &notifyTestSMTPServer{listener: ln}
	go srv.serve(tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}}))
	t.Cleanup(func() { _ = ln.Close() })
	return srv
}

// parseSMTPAddr extracts the address from "MAIL FROM:<a@x> PARAMS" shapes.
func parseSMTPAddr(rest string) string {
	if i := strings.IndexByte(rest, '>'); i >= 0 {
		rest = rest[:i]
	}
	return strings.Trim(strings.TrimPrefix(rest, ":"), " <>")
}

func (s *notifyTestSMTPServer) hostPort() (string, int) {
	host, portStr, _ := net.SplitHostPort(s.listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	return host, port
}

func (s *notifyTestSMTPServer) serve(ln net.Listener) {
	conn, err := ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := textproto.NewConn(conn)
	reply := func(code int, lines ...string) {
		for i, l := range lines {
			if i == len(lines)-1 {
				_, _ = fmt.Fprintf(w.W, "%d %s\r\n", code, l)
			} else {
				_, _ = fmt.Fprintf(w.W, "%d-%s\r\n", code, l)
			}
		}
		_ = w.W.Flush() // textproto.Conn has no Printf here; flush the embedded writer
	}
	reply(220, "fake-smtp ready")
	inData := false
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if inData {
			if line == "." {
				inData = false
				reply(250, "ok")
			} else {
				if strings.HasPrefix(line, ".") { // dot-stuffing reversal
					line = line[1:]
				}
				s.Data = append(s.Data, []byte(line+"\r\n")...)
			}
			continue
		}
		cmd := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
			reply(250, "fake-smtp", "AUTH PLAIN", "8BITMIME")
		case strings.HasPrefix(cmd, "AUTH PLAIN"):
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				decoded, derr := base64.StdEncoding.DecodeString(parts[2])
				if derr == nil {
					fields := strings.Split(string(decoded), "\x00")
					if len(fields) == 3 {
						s.Authed = fields[1]
					}
				}
			}
			reply(235, "ok")
		case strings.HasPrefix(cmd, "MAIL FROM:"):
			s.MailFrom = parseSMTPAddr(line[len("MAIL FROM:"):])
			reply(250, "ok")
		case strings.HasPrefix(cmd, "RCPT TO:"):
			s.RcptTo = append(s.RcptTo, parseSMTPAddr(line[len("RCPT TO:"):]))
			reply(250, "ok")
		case cmd == "DATA":
			inData = true
			s.Data = nil
			reply(354, "go")
		case cmd == "QUIT":
			reply(221, "bye")
			return
		default:
			reply(502, "unknown command")
		}
	}
}

func TestEmailSendRealSMTPPath(t *testing.T) {
	fake := notifyStartFakeSMTP(t)
	host, port := fake.hostPort()
	node := &EmailSend{BaseNode: BaseNode{ID: "n"}, Credential: "smtp",
		To: []any{"a@x.com", "b@x.com"}, Subject: "日报", Body: "完成 3 项",
		Credentials: notifyTestCredentialStore{"smtp": {
			"host": host, "port": port, "username": "bot@test", "password": "pw",
		}}}
	res, err := node.Execute(notifyTestContext(t, nil, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Result.(map[string]any)["status"] != "sent" {
		t.Errorf("result = %#v", res.Result)
	}
	if fake.Authed != "bot@test" {
		t.Errorf("auth user = %q", fake.Authed)
	}
	if fake.MailFrom != "bot@test" {
		t.Errorf("mail from = %q", fake.MailFrom)
	}
	if len(fake.RcptTo) != 2 || fake.RcptTo[0] != "a@x.com" || fake.RcptTo[1] != "b@x.com" {
		t.Errorf("rcpt to = %v", fake.RcptTo)
	}
	msg := string(fake.Data)
	if !strings.Contains(msg, "To: a@x.com, b@x.com") {
		t.Errorf("To header missing:\n%s", msg)
	}
	if !strings.Contains(msg, "Subject: =?utf-8?b?") {
		t.Errorf("non-ASCII subject not RFC2047-encoded:\n%s", msg)
	}
	if !strings.Contains(msg, "Content-Transfer-Encoding: quoted-printable") {
		t.Errorf("body not QP:\n%s", msg)
	}
	_, plain, _ := notifyDecodeTestMessage(t, fake.Data)
	if strings.TrimSpace(plain) != "完成 3 项" {
		t.Errorf("decoded plain = %q", plain)
	}
}

func TestEmailSendSendsOverImplicitTLS(t *testing.T) {
	cert, err := notifySelfSignedCert("localhost")
	if err != nil {
		t.Fatal(err)
	}
	fake := notifyStartFakeSMTPS(t, *cert)
	host, port := fake.hostPort()
	node := &EmailSend{BaseNode: BaseNode{ID: "n"}, Credential: "smtp",
		To: "a@x.com", Body: "hi",
		Credentials: notifyTestCredentialStore{"smtp": {
			"host": host, "port": port, "use_ssl": true,
			"username": "u@x.com", "password": "pw",
		}}}
	if _, err := node.Execute(notifyTestContext(t, nil, nil)); err != nil {
		t.Fatalf("implicit TLS send: %v", err)
	}
	if !strings.Contains(string(fake.Data), "To: a@x.com") {
		t.Errorf("message over TLS:\n%s", fake.Data)
	}
}

// notifySelfSignedCert mints an in-memory self-signed cert (test-only TLS).
func notifySelfSignedCert(host string) (*tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{host},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return nil, err
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		return nil, err
	}
	return &cert, nil
}

// TestEmailSendJSONRoundTrip checks the deploy-JSON unmarshal shape.
func TestEmailSendJSONRoundTrip(t *testing.T) {
	doc := `{"type":"email_send","id":"e1","credential":"smtp","to":"$INPUT.to",
		"subject":"$INPUT.subject","body":"$INPUT.body","body_is_html":true,"dry_run":true}`
	node := new(EmailSend)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.GetType() != nodeTypeEmailSend {
		t.Errorf("GetType = %v", node.GetType())
	}
	if node.To != "$INPUT.to" || node.Subject != "$INPUT.subject" ||
		node.Body != "$INPUT.body" || !node.BodyIsHTML || !node.DryRun {
		t.Errorf("fields = %#v", node)
	}
}
