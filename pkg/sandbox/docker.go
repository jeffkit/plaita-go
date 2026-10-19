package sandbox

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DockerDriver implements Driver against the Docker Engine HTTP API over a
// Unix socket (stdlib net/http; zero external dependencies). The design
// mirrors sandbox_docker.py where it matters:
//
//   - one data directory per (execution_id, ws_key), bind-mounted at /work;
//   - the compute layer is disposable: Release stops + removes the container,
//     the data layer survives unless the caller asks to drop it;
//   - credentials never enter argv or the container environment: they ride a
//     0600 envfile injected through the archive (tar) endpoint;
//   - the container is named deterministically, so a crashed driver can
//     re-attach and a stale container can be cleaned up.
//
// Unlike the Python driver (per-exec `docker run`), this driver keeps one
// container per handle and uses the exec API, which is what the Docker HTTP
// surface makes cheap. The image must therefore provide a long-lived command
// (`sleep infinity`) plus the agent CLI and git.
type DockerDriver struct {
	Socket   string
	DataRoot string

	httpClient *http.Client
	dialer     net.Dialer
}

func init() {
	RegisterDriver("docker", func() Driver { return NewDockerDriverFromEnv() })
}

// NewDockerDriverFromEnv builds a driver from PLAITA_DOCKER_HOST /
// DOCKER_HOST (unix only) and PLAITA_DOCKER_DATA_ROOT.
func NewDockerDriverFromEnv() *DockerDriver {
	return NewDockerDriver(dockerSocketFromEnv(), dockerDataRootFromEnv())
}

// NewDockerDriver builds a driver rooted at an explicit socket/data root.
func NewDockerDriver(socket, dataRoot string) *DockerDriver {
	if strings.TrimSpace(socket) == "" {
		socket = dockerSocketFromEnv()
	}
	socket = strings.TrimPrefix(socket, "unix://")
	if strings.TrimSpace(dataRoot) == "" {
		dataRoot = dockerDataRootFromEnv()
	}
	if abs, err := filepath.Abs(dataRoot); err == nil {
		dataRoot = abs
	}
	d := &DockerDriver{Socket: socket, DataRoot: dataRoot}
	d.httpClient = &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return d.dialer.DialContext(ctx, "unix", d.Socket)
			},
			DisableCompression: true,
			MaxIdleConns:       4,
			IdleConnTimeout:    30 * time.Second,
		},
	}
	return d
}

func dockerSocketFromEnv() string {
	for _, key := range []string{"PLAITA_DOCKER_HOST", "DOCKER_HOST"} {
		value := strings.TrimSpace(os.Getenv(key))
		if value == "" {
			continue
		}
		if strings.HasPrefix(value, "unix://") || strings.HasPrefix(value, "/") {
			return strings.TrimPrefix(value, "unix://")
		}
		// tcp:// daemons are out of scope for the MVP. Return the value so the
		// Unix dial fails loudly instead of silently using the default socket.
		return value
	}
	return "/var/run/docker.sock"
}

func dockerDataRootFromEnv() string {
	if value := strings.TrimSpace(os.Getenv("PLAITA_DOCKER_DATA_ROOT")); value != "" {
		return value
	}
	return filepath.Join(DefaultHome(), "sandbox-data")
}

// Name implements Driver.
func (d *DockerDriver) Name() string { return "docker" }

// --- API plumbing --------------------------------------------------------

// APIError is a non-2xx Docker API response.
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("docker API status %d", e.Status)
	}
	return fmt.Sprintf("docker API status %d: %s", e.Status, e.Message)
}

// IsNotFound reports a Docker 404 (missing container/image/exec).
func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound
}

func (d *DockerDriver) do(ctx context.Context, method, requestPath string, body []byte, contentType string) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+requestPath, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		if contentType == "" {
			contentType = "application/json"
		}
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	message := strings.TrimSpace(string(data))
	var envelope struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(data, &envelope) == nil && envelope.Message != "" {
		message = envelope.Message
	}
	return nil, &APIError{Status: resp.StatusCode, Message: message}
}

func (d *DockerDriver) doJSON(ctx context.Context, method, requestPath string, in any, out any) error {
	var body []byte
	if in != nil {
		encoded, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = encoded
	}
	resp, err := d.do(ctx, method, requestPath, body, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	return json.Unmarshal(data, out)
}

// escapeName keeps slashes in image references while escaping the rest.
func escapeName(name string) string {
	return strings.ReplaceAll(url.PathEscape(name), "%2F", "/")
}

// --- inspect / lifecycle -------------------------------------------------

type containerInspect struct {
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	Config struct {
		Image  string            `json:"Image"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State struct {
		Running bool   `json:"Running"`
		Paused  bool   `json:"Paused"`
		Status  string `json:"Status"`
	} `json:"State"`
}

const (
	labelEngine      = "com.plaita.sandbox.engine"
	labelExecutionID = "com.plaita.sandbox.execution_id"
	labelWorkspace   = "com.plaita.sandbox.ws_key"
	labelWorkspaceNm = "com.plaita.sandbox.workspace"
	engineName       = "plaita-go"
)

func (d *DockerDriver) inspectContainer(ctx context.Context, name string) (*containerInspect, bool, error) {
	var out containerInspect
	err := d.doJSON(ctx, http.MethodGet, "/containers/"+escapeName(name)+"/json", nil, &out)
	if IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &out, true, nil
}

func (d *DockerDriver) ensureImage(ctx context.Context, image string) error {
	err := d.doJSON(ctx, http.MethodGet, "/images/"+escapeName(image)+"/json", nil, nil)
	if err == nil {
		return nil
	}
	if !IsNotFound(err) {
		return err
	}
	repo, tag := splitImageRef(image)
	query := url.Values{}
	query.Set("fromImage", repo)
	query.Set("tag", tag)
	resp, err := d.do(ctx, http.MethodPost, "/images/create?"+query.Encode(), nil, "")
	if err != nil {
		return fmt.Errorf("%w: pull %s: %v", ErrImageMissing, image, err)
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	for {
		var update struct {
			Error string `json:"error"`
		}
		if err := decoder.Decode(&update); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("%w: pull %s: %v", ErrImageMissing, image, err)
		}
		if update.Error != "" {
			return fmt.Errorf("%w: pull %s: %s", ErrImageMissing, image, update.Error)
		}
	}
	return nil
}

func splitImageRef(ref string) (string, string) {
	if idx := strings.Index(ref, "@"); idx >= 0 {
		return ref[:idx], ref[idx+1:]
	}
	slash := strings.LastIndex(ref, "/")
	if colon := strings.LastIndex(ref, ":"); colon > slash {
		return ref[:colon], ref[colon+1:]
	}
	return ref, "latest"
}

func (d *DockerDriver) startContainer(ctx context.Context, name string) error {
	err := d.doJSON(ctx, http.MethodPost, "/containers/"+escapeName(name)+"/start", nil, nil)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotModified {
			return nil
		}
		return err
	}
	return nil
}

// --- ensure --------------------------------------------------------------

// Ensure idempotently attaches or creates the workspace container and
// provisions the data directory.
func (d *DockerDriver) Ensure(ctx context.Context, spec *WorkspaceSpec, executionID, wsKey string) (*WorkspaceHandle, error) {
	if spec == nil {
		return nil, fmt.Errorf("%w: nil workspace spec", ErrConfig)
	}
	if err := EnsureImagePinned(spec); err != nil {
		return nil, err
	}
	if spec.Image == "" {
		return nil, fmt.Errorf("%w: sandbox %q: docker driver needs image (template is not supported)",
			ErrConfig, spec.Name)
	}

	datadir := filepath.Join(d.DataRoot, ResourceName(executionID, wsKey, "data"))
	if err := d.prepareDataDir(datadir, spec, executionID, wsKey); err != nil {
		return nil, err
	}
	container := ResourceName(executionID, wsKey, "ctr")
	containerID, err := d.ensureContainer(ctx, spec, executionID, wsKey, container, datadir)
	if err != nil {
		return nil, err
	}

	handle := &WorkspaceHandle{
		Driver:      d.Name(),
		ID:          HandleID(executionID, wsKey),
		Path:        SandboxPath,
		Key:         wsKey,
		ExecutionID: executionID,
		Data: map[string]string{
			"datadir":      datadir,
			"container":    container,
			"container_id": containerID,
			"image":        spec.Image,
		},
	}
	if err := d.provision(ctx, handle, spec); err != nil {
		// The container is long-lived (unlike the Python per-exec run), so a
		// failed ensure must clean it up instead of leaking a running box.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		_ = d.Release(cleanupCtx, handle, ReleaseOptions{KeepData: true, Failed: true, FailureReason: err.Error()})
		cancel()
		return nil, err
	}
	writeSidecar(datadir, d.Name(), executionID, wsKey)
	return handle, nil
}

func (d *DockerDriver) prepareDataDir(datadir string, spec *WorkspaceSpec, executionID, wsKey string) error {
	if err := os.MkdirAll(datadir, 0o755); err != nil {
		return fmt.Errorf("sandbox data dir: %w", err)
	}
	metaPath := d.metadataPath(datadir)
	meta, err := readMetadata(metaPath)
	if err != nil {
		return err
	}
	if meta != nil {
		if meta.Engine != engineName {
			return fmt.Errorf("%w: data dir %s belongs to engine %q, refusing to reuse it",
				ErrConfig, datadir, meta.Engine)
		}
		if meta.ExecutionID != executionID || meta.WSKey != wsKey {
			return fmt.Errorf("%w: data dir %s belongs to %s:%s, refusing to reuse it",
				ErrConfig, datadir, meta.ExecutionID, meta.WSKey)
		}
		return nil
	}
	entries, err := os.ReadDir(datadir)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("%w: data dir %s is non-empty but has no %s marker; "+
			"refusing to reuse a foreign working copy", ErrConfig, datadir, engineName)
	}
	return writeMetadata(metaPath, sandboxMetadata{
		Engine:      engineName,
		Driver:      d.Name(),
		ExecutionID: executionID,
		WSKey:       wsKey,
		Workspace:   spec.Name,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	})
}

func (d *DockerDriver) metadataPath(datadir string) string {
	return filepath.Join(d.DataRoot, ".plaita-go", filepath.Base(datadir)+".json")
}

type sandboxMetadata struct {
	Engine      string `json:"engine"`
	Driver      string `json:"driver"`
	ExecutionID string `json:"execution_id"`
	WSKey       string `json:"ws_key"`
	Workspace   string `json:"workspace"`
	CreatedAt   string `json:"created_at"`
}

func readMetadata(path string) (*sandboxMetadata, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var meta sandboxMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("%w: corrupt sandbox metadata %s: %v", ErrConfig, path, err)
	}
	return &meta, nil
}

func writeMetadata(path string, meta sandboxMetadata) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}

func (d *DockerDriver) ensureContainer(ctx context.Context, spec *WorkspaceSpec, executionID, wsKey, name, datadir string) (string, error) {
	existing, found, err := d.inspectContainer(ctx, name)
	if err != nil {
		return "", err
	}
	if found {
		if existing.Config.Labels[labelEngine] != engineName {
			return "", fmt.Errorf("%w: container %s exists but is not a %s sandbox "+
				"(name collision — set PLAITA_SANDBOX_PREFIX to isolate this deployment)",
				ErrConfig, name, engineName)
		}
		if existing.Config.Labels[labelExecutionID] != executionID ||
			existing.Config.Labels[labelWorkspace] != wsKey {
			return "", fmt.Errorf("%w: container %s carries a different workspace identity "+
				"(%s/%s)", ErrConfig, name,
				existing.Config.Labels[labelExecutionID], existing.Config.Labels[labelWorkspace])
		}
		// A dead/removing/paused container cannot be started again; recreate
		// it (the data layer survives).
		recreate := existing.Config.Image != spec.Image ||
			existing.State.Paused ||
			existing.State.Status == "dead" ||
			existing.State.Status == "removing"
		if !recreate {
			if err := d.startContainer(ctx, name); err != nil {
				return "", err
			}
			return existing.ID, nil
		}
		if err := d.removeContainer(ctx, name); err != nil {
			return "", err
		}
	}

	if err := d.ensureImage(ctx, spec.Image); err != nil {
		return "", err
	}
	hostConfig, err := d.buildHostConfig(spec, datadir)
	if err != nil {
		return "", err
	}
	request := map[string]any{
		"Image":      spec.Image,
		"Cmd":        []string{"sleep", "infinity"},
		"WorkingDir": SandboxPath,
		"Env": []string{
			"PLAITA_SANDBOX=1",
			"PLAITA_EXECUTION_ID=" + executionID,
			"PLAITA_WORKSPACE=" + wsKey,
		},
		"Labels": map[string]string{
			labelEngine:      engineName,
			labelExecutionID: executionID,
			labelWorkspace:   wsKey,
			labelWorkspaceNm: spec.Name,
		},
		"HostConfig": hostConfig,
	}
	var created struct {
		ID       string   `json:"Id"`
		Warnings []string `json:"Warnings"`
	}
	err = d.doJSON(ctx, http.MethodPost, "/containers/create?name="+url.QueryEscape(name), request, &created)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict {
			// Lost a create race: attach to the winner.
			existing, found, inspectErr := d.inspectContainer(ctx, name)
			if inspectErr != nil {
				return "", inspectErr
			}
			if !found {
				return "", err
			}
			if existing.Config.Labels[labelEngine] != engineName {
				return "", fmt.Errorf("%w: container %s name collision", ErrConfig, name)
			}
			if startErr := d.startContainer(ctx, name); startErr != nil {
				return "", startErr
			}
			return existing.ID, nil
		}
		return "", fmt.Errorf("create sandbox container %s: %w", name, err)
	}
	if err := d.startContainer(ctx, name); err != nil {
		return "", err
	}
	return created.ID, nil
}

func (d *DockerDriver) buildHostConfig(spec *WorkspaceSpec, datadir string) (map[string]any, error) {
	binds := []string{datadir + ":" + SandboxPath}
	network := "bridge"
	switch spec.Egress {
	case "", "bridge":
	case "none":
		network = "none"
	default:
		return nil, fmt.Errorf("%w: sandbox %q unsupported egress %q", ErrConfig, spec.Name, spec.Egress)
	}
	hostConfig := map[string]any{
		"Binds":       binds,
		"NetworkMode": network,
		"Init":        true,
		"AutoRemove":  false,
	}
	if cpus, ok, err := parseCPUs(spec.Resources["cpus"]); err != nil {
		return nil, err
	} else if ok {
		hostConfig["NanoCpus"] = cpus
	}
	if memory, ok, err := parseMemory(spec.Resources["memory"]); err != nil {
		return nil, err
	} else if ok {
		hostConfig["Memory"] = memory
	}
	return hostConfig, nil
}

func (d *DockerDriver) provision(ctx context.Context, handle *WorkspaceHandle, spec *WorkspaceSpec) error {
	repo := spec.GitRepo()
	if repo == "" {
		return nil
	}
	probe, err := d.Exec(ctx, handle, ExecOptions{
		Cmd:     []string{"git", "-C", handle.Path, "rev-parse", "--is-inside-work-tree"},
		WorkDir: handle.Path,
		Env:     map[string]string{"GIT_TERMINAL_PROMPT": "0"},
		Timeout: 30 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("provision probe: %w", err)
	}
	if probe.ExitCode != 0 {
		// The data dir is driver-owned: clear it before cloning. Clear the
		// CONTENTS (not the dir) — virtiofs-backed mounts cache removed paths.
		if _, err := d.Exec(ctx, handle, ExecOptions{
			Cmd:     []string{"sh", "-c", "rm -rf /work/.[!.]* /work/*"},
			WorkDir: handle.Path,
			Timeout: 60 * time.Second,
		}); err != nil {
			return fmt.Errorf("provision clean: %w", err)
		}
		clone := []string{"git", "-c", "safe.directory=*", "clone"}
		if branch := spec.GitBranch(); branch != "" {
			clone = append(clone, "--branch", branch, "--single-branch")
		}
		clone = append(clone, repo, ".")
		result, err := d.Exec(ctx, handle, ExecOptions{
			Cmd:     clone,
			WorkDir: handle.Path,
			Env:     map[string]string{"GIT_TERMINAL_PROMPT": "0"},
			Timeout: 10 * time.Minute,
		})
		if err != nil {
			return fmt.Errorf("provision clone: %w", err)
		}
		if result.ExitCode != 0 {
			return fmt.Errorf("provision clone %s failed (exit %d): %s",
				repo, result.ExitCode, tail(result.Stderr, 400))
		}
	}
	// Bind-mounted checkouts can map ownership to the host user; git inside
	// the container (root) then trips dubious-ownership checks.
	_, _ = d.Exec(ctx, handle, ExecOptions{
		Cmd:     []string{"git", "config", "--global", "--add", "safe.directory", "*"},
		Env:     map[string]string{"GIT_TERMINAL_PROMPT": "0"},
		Timeout: 30 * time.Second,
	})
	return nil
}

func tail(text string, n int) string {
	text = strings.TrimSpace(text)
	if len(text) <= n {
		return text
	}
	return text[len(text)-n:]
}

// --- exec ----------------------------------------------------------------

type execCreateResponse struct {
	ID string `json:"Id"`
}

type execInspect struct {
	Running  bool `json:"Running"`
	ExitCode *int `json:"ExitCode"`
}

// Exec runs one command in the workspace container.
func (d *DockerDriver) Exec(ctx context.Context, handle *WorkspaceHandle, opts ExecOptions) (*ExecResult, error) {
	if handle == nil {
		return nil, fmt.Errorf("%w: nil workspace handle", ErrConfig)
	}
	if len(opts.Cmd) == 0 {
		return nil, fmt.Errorf("%w: empty exec command", ErrConfig)
	}
	container := handle.Data["container_id"]
	if container == "" {
		container = handle.Data["container"]
	}
	if container == "" {
		return nil, fmt.Errorf("%w: handle has no container", ErrConfig)
	}

	execCtx := ctx
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	cmd := opts.Cmd
	if opts.EnvFile != "" || opts.StdinFile != "" {
		cmd = wrapWithEnvFile(opts)
	}
	env := make([]string, 0, len(opts.Env))
	for key, value := range opts.Env {
		env = append(env, key+"="+value)
	}
	sort.Strings(env)

	createBody := map[string]any{
		"AttachStdout": true,
		"AttachStderr": true,
		"AttachStdin":  false,
		"Tty":          false,
		"Cmd":          cmd,
	}
	if len(env) > 0 {
		createBody["Env"] = env
	}
	if opts.WorkDir != "" {
		createBody["WorkingDir"] = opts.WorkDir
	}
	var created execCreateResponse
	if err := d.doJSON(execCtx, http.MethodPost, "/containers/"+escapeName(container)+"/exec", createBody, &created); err != nil {
		return nil, fmt.Errorf("sandbox exec create: %w", err)
	}
	if created.ID == "" {
		return nil, fmt.Errorf("sandbox exec create: empty exec id")
	}

	stdout, stderr, streamErr := d.startExec(execCtx, created.ID)
	if streamErr != nil {
		if errors.Is(streamErr, context.DeadlineExceeded) || errors.Is(streamErr, context.Canceled) {
			d.killContainer(context.WithoutCancel(ctx), container)
		}
		return nil, fmt.Errorf("sandbox exec stream: %w", streamErr)
	}

	// The stream closed, so the process is done or about to be; inspect on a
	// fresh short budget so a wall clock that expired exactly at exit does not
	// turn a successful exec into a timeout.
	inspectCtx, cancelInspect := context.WithTimeout(context.WithoutCancel(execCtx), 5*time.Second)
	exitCode, inspectErr := d.inspectExec(inspectCtx, created.ID)
	cancelInspect()
	if inspectErr != nil {
		return nil, fmt.Errorf("sandbox exec inspect: %w", inspectErr)
	}
	return &ExecResult{ExitCode: exitCode, Stdout: stdout, Stderr: stderr}, nil
}

func wrapWithEnvFile(opts ExecOptions) []string {
	// POSIX loader: `export "$line"` takes the whole KEY=VALUE literally (no
	// word splitting / expansion), unlike `set -a; . file`. Values are
	// pre-validated to be newline/NUL-free.
	script := "set -a; " +
		"while IFS= read -r line || [ -n \"$line\" ]; do " +
		"case \"$line\" in ''|'#'*) continue;; esac; export \"$line\"; done < " +
		shellQuote(opts.EnvFile) + "; set +a; "
	if opts.StdinFile != "" {
		script += "exec \"$@\" < " + shellQuote(opts.StdinFile)
	} else {
		script += "exec \"$@\""
	}
	cmd := []string{"/bin/sh", "-c", script, "plaita-sandbox-exec"}
	return append(cmd, opts.Cmd...)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func (d *DockerDriver) startExec(ctx context.Context, execID string) (string, string, error) {
	conn, err := d.dialer.DialContext(ctx, "unix", d.Socket)
	if err != nil {
		return "", "", err
	}
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer func() {
		stopClose()
		_ = conn.Close()
	}()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	payload, _ := json.Marshal(map[string]any{"Detach": false, "Tty": false})
	request := "POST /exec/" + url.PathEscape(execID) + "/start HTTP/1.1\r\n" +
		"Host: docker\r\n" +
		"Content-Type: application/json\r\n" +
		"Content-Length: " + strconv.Itoa(len(payload)) + "\r\n" +
		"Connection: Upgrade\r\n" +
		"Upgrade: tcp\r\n\r\n"
	if _, err := io.WriteString(conn, request); err != nil {
		return "", "", err
	}
	if _, err := conn.Write(payload); err != nil {
		return "", "", err
	}

	reader := bufio.NewReader(conn)
	textReader := textproto.NewReader(reader)
	statusLine, err := textReader.ReadLine()
	if err != nil {
		return "", "", execStreamError(ctx, err)
	}
	parts := strings.SplitN(statusLine, " ", 3)
	if len(parts) < 2 {
		return "", "", fmt.Errorf("malformed docker exec response: %q", statusLine)
	}
	status, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("malformed docker exec status: %q", statusLine)
	}
	if _, err := textReader.ReadMIMEHeader(); err != nil {
		return "", "", execStreamError(ctx, err)
	}
	if status != http.StatusOK && status != http.StatusSwitchingProtocols {
		body, _ := io.ReadAll(io.LimitReader(reader, 4096))
		return "", "", &APIError{Status: status, Message: strings.TrimSpace(string(body))}
	}

	stream := io.Reader(reader)
	var stdout, stderr bytes.Buffer
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(stream, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return stdout.String(), stderr.String(), execStreamError(ctx, err)
		}
		size := binary.BigEndian.Uint32(header[4:])
		if size > 64<<20 {
			return stdout.String(), stderr.String(), fmt.Errorf("docker exec frame too large: %d", size)
		}
		frame := make([]byte, size)
		if _, err := io.ReadFull(stream, frame); err != nil {
			return stdout.String(), stderr.String(), execStreamError(ctx, err)
		}
		switch header[0] {
		case 1:
			stdout.Write(frame)
		case 2:
			stderr.Write(frame)
		default:
			// stdin/control frames are not read by this host
		}
	}
	return stdout.String(), stderr.String(), nil
}

func execStreamError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return context.DeadlineExceeded
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return context.DeadlineExceeded
	}
	// The after-func closes the connection when the context expires; the
	// resulting net.ErrClosed can race slightly ahead of ctx.Err() becoming
	// observable in this goroutine. Attribute it to the deadline.
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return err
}

func (d *DockerDriver) inspectExec(ctx context.Context, execID string) (int, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		var out execInspect
		err := d.doJSON(ctx, http.MethodGet, "/exec/"+url.PathEscape(execID)+"/json", nil, &out)
		if err != nil {
			return 0, err
		}
		if !out.Running {
			if out.ExitCode != nil {
				return *out.ExitCode, nil
			}
			return 0, nil
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("exec %s still running after the stream closed", execID)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// --- files ---------------------------------------------------------------

// InjectFile writes data to containerPath through a tar stream. The parent
// directory is created when needed.
func (d *DockerDriver) InjectFile(ctx context.Context, handle *WorkspaceHandle, containerPath string, data []byte, mode int64) error {
	container := handle.Data["container_id"]
	if container == "" {
		container = handle.Data["container"]
	}
	parent := path.Dir(containerPath)
	base := path.Base(containerPath)
	if parent == "." || base == "/" || base == "." {
		return fmt.Errorf("%w: invalid container path %q", ErrConfig, containerPath)
	}
	if parent != "/tmp" && parent != "/" {
		if _, err := d.Exec(ctx, handle, ExecOptions{
			Cmd:     []string{"mkdir", "-p", parent},
			Timeout: 30 * time.Second,
		}); err != nil {
			return fmt.Errorf("sandbox inject mkdir: %w", err)
		}
	}

	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	header := &tar.Header{
		Name: base,
		Mode: mode,
		Size: int64(len(data)),
		// UTC, zero time keeps the archive deterministic (tests)
		ModTime: time.Unix(0, 0),
	}
	if err := tw.WriteHeader(header); err != nil {
		return err
	}
	if _, err := tw.Write(data); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	requestPath := "/containers/" + escapeName(container) + "/archive?path=" + url.QueryEscape(parent)
	resp, err := d.do(ctx, http.MethodPut, requestPath, archive.Bytes(), "application/x-tar")
	if err != nil {
		return fmt.Errorf("sandbox inject %s: %w", containerPath, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// RemoveFile removes a file inside the container.
func (d *DockerDriver) RemoveFile(ctx context.Context, handle *WorkspaceHandle, containerPath string) error {
	_, err := d.Exec(ctx, handle, ExecOptions{
		Cmd:     []string{"rm", "-f", containerPath},
		Timeout: 30 * time.Second,
	})
	return err
}

// --- kill / release ------------------------------------------------------

// Enforce kills the container (Docker has no per-exec kill; killing the
// container terminates every process in its PID namespace).
func (d *DockerDriver) Enforce(ctx context.Context, handle *WorkspaceHandle) error {
	if handle == nil {
		return nil
	}
	container := handle.Data["container_id"]
	if container == "" {
		container = handle.Data["container"]
	}
	if container == "" {
		return nil
	}
	d.killContainer(ctx, container)
	return nil
}

func (d *DockerDriver) killContainer(ctx context.Context, container string) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Cleanup must run even when the caller's context just expired (that is
	// precisely the timeout path).
	killCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	// Best effort: 404 (already gone) and 409 (already stopped) are normal.
	_ = d.doJSON(killCtx, http.MethodPost,
		"/containers/"+escapeName(container)+"/kill?signal=SIGKILL", nil, nil)
}

func (d *DockerDriver) removeContainer(ctx context.Context, container string) error {
	err := d.doJSON(ctx, http.MethodDelete,
		"/containers/"+escapeName(container)+"?force=1&v=0", nil, nil)
	if err != nil && !IsNotFound(err) {
		return err
	}
	return nil
}

// Release stops and removes the compute layer. Failure scenes are retained:
// a marker is written into the data directory before the container goes away.
func (d *DockerDriver) Release(ctx context.Context, handle *WorkspaceHandle, opts ReleaseOptions) error {
	if handle == nil {
		return nil
	}
	if ctx == nil || ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
	}
	container := handle.Data["container_id"]
	if container == "" {
		container = handle.Data["container"]
	}
	datadir := handle.Data["datadir"]

	if opts.Failed && datadir != "" {
		writeFailureMarker(datadir, handle, opts.FailureReason)
	}
	if !opts.KeepData && datadir != "" {
		if _, err := d.Exec(ctx, handle, ExecOptions{
			Cmd:     []string{"sh", "-c", "rm -rf /work/.[!.]* /work/*"},
			Timeout: 60 * time.Second,
		}); err != nil {
			// continue: host-side removal still gets the rest
		}
	}

	var firstErr error
	if container != "" {
		stopCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := d.doJSON(stopCtx, http.MethodPost,
			"/containers/"+escapeName(container)+"/stop?t=5", nil, nil)
		cancel()
		if err != nil && !IsNotFound(err) {
			var apiErr *APIError
			if !errors.As(err, &apiErr) ||
				(apiErr.Status != http.StatusNotModified && apiErr.Status != http.StatusConflict) {
				firstErr = err
			}
		}
		if err := d.removeContainer(ctx, container); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if !opts.KeepData && datadir != "" {
		if err := os.RemoveAll(datadir); err != nil {
			// root-owned files: an unremoved data dir is not a flow failure
			_ = err
		}
		if err := os.Remove(d.metadataPath(datadir)); err != nil && !errors.Is(err, os.ErrNotExist) {
			_ = err
		}
	}
	return firstErr
}

func writeFailureMarker(datadir string, handle *WorkspaceHandle, reason string) {
	if strings.TrimSpace(reason) == "" {
		reason = "agent exec failed"
	}
	marker := map[string]any{
		"driver":       handle.Driver,
		"execution_id": handle.ExecutionID,
		"ws_key":       handle.Key,
		"reason":       reason,
		"at":           time.Now().UTC().Format(time.RFC3339),
	}
	data, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return
	}
	// Keep the marker out of the agent's working-tree status when the data
	// layer is a git checkout; fall back to the root for non-git sandboxes.
	target := filepath.Join(datadir, ".plaita-sandbox-failed.json")
	if info, err := os.Stat(filepath.Join(datadir, ".git")); err == nil && info.IsDir() {
		target = filepath.Join(datadir, ".git", "plaita-sandbox-failed.json")
	}
	_ = os.MkdirAll(filepath.Dir(target), 0o755)
	_ = os.WriteFile(target, append(data, '\n'), 0o600)
}

// writeSidecar writes the reaper-visible metadata, preferring .git/ so the
// file never shows up in the agent's working-tree status.
func writeSidecar(datadir, driver, executionID, wsKey string) {
	gitDir := filepath.Join(datadir, ".git")
	target := filepath.Join(datadir, ".plaita-sandbox.json")
	if info, err := os.Stat(gitDir); err == nil && info.IsDir() {
		target = filepath.Join(gitDir, "plaita-sandbox.json")
	}
	if _, err := os.Stat(target); err == nil {
		return
	}
	payload := map[string]any{
		"engine":       engineName,
		"driver":       driver,
		"execution_id": executionID,
		"ws_key":       wsKey,
		"created_at":   time.Now().UTC().Format(time.RFC3339),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	_ = os.WriteFile(target, data, 0o600)
}

// --- resource parsing ----------------------------------------------------

func parseCPUs(value any) (int64, bool, error) {
	switch v := value.(type) {
	case nil:
		return 0, false, nil
	case float64:
		return int64(v * 1e9), true, nil
	case int:
		return int64(v) * 1e9, true, nil
	case int64:
		return v * 1e9, true, nil
	case string:
		if strings.TrimSpace(v) == "" {
			return 0, false, nil
		}
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, false, fmt.Errorf("%w: resources.cpus %q: %v", ErrConfig, v, err)
		}
		return int64(parsed * 1e9), true, nil
	default:
		return 0, false, fmt.Errorf("%w: resources.cpus has unsupported type %T", ErrConfig, value)
	}
}

func parseMemory(value any) (int64, bool, error) {
	switch v := value.(type) {
	case nil:
		return 0, false, nil
	case float64:
		return int64(v), true, nil
	case int:
		return int64(v), true, nil
	case int64:
		return v, true, nil
	case string:
		return parseByteSize(v)
	default:
		return 0, false, fmt.Errorf("%w: resources.memory has unsupported type %T", ErrConfig, value)
	}
}

func parseByteSize(raw string) (int64, bool, error) {
	text := strings.ToLower(strings.TrimSpace(raw))
	if text == "" {
		return 0, false, nil
	}
	units := []struct {
		suffix string
		scale  float64
	}{
		{"gib", 1 << 30}, {"mib", 1 << 20}, {"kib", 1 << 10},
		{"gb", 1e9}, {"mb", 1e6}, {"kb", 1e3},
		{"g", 1 << 30}, {"m", 1 << 20}, {"k", 1 << 10}, {"b", 1},
	}
	for _, unit := range units {
		if strings.HasSuffix(text, unit.suffix) {
			number := strings.TrimSpace(strings.TrimSuffix(text, unit.suffix))
			parsed, err := strconv.ParseFloat(number, 64)
			if err != nil {
				return 0, false, fmt.Errorf("%w: resources.memory %q: %v", ErrConfig, raw, err)
			}
			return int64(parsed * unit.scale), true, nil
		}
	}
	parsed, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, false, fmt.Errorf("%w: resources.memory %q: %v", ErrConfig, raw, err)
	}
	return int64(parsed), true, nil
}
