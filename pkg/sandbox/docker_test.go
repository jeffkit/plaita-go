package sandbox

import (
	"archive/tar"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeDocker is a minimal Docker Engine API server on a Unix socket. It lets
// the driver's real HTTP/tar/hijack code paths run without a Docker daemon.
type fakeDocker struct {
	t *testing.T

	mu         sync.Mutex
	images     map[string]bool
	containers map[string]*fakeContainer
	execs      map[string]*fakeExecState
	archives   []fakeArchive
	creates    []map[string]any
	execCreate []map[string]any
	requests   []string
	nextID     int
	gitReady   bool
	execHook   func(cmd []string) (stdout, stderr string, exit int)
}

type fakeContainer struct {
	id      string
	name    string
	image   string
	labels  map[string]string
	running bool
}

type fakeExecState struct {
	cmd     []string
	env     []string
	workdir string
	stdout  string
	stderr  string
	exit    int
	started bool
}

type fakeArchive struct {
	path string
	name string
	mode int64
	data []byte
}

func startFakeDocker(t *testing.T) (*DockerDriver, *fakeDocker) {
	t.Helper()
	dir := t.TempDir()
	// Unix socket paths are capped (~104 bytes on macOS); t.TempDir() is too
	// long here, so use a short /tmp name.
	socket := fmt.Sprintf("/tmp/plsbx-%d-%d.sock", os.Getpid(), time.Now().UnixNano())
	_ = os.Remove(socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeDocker{
		t:          t,
		images:     map[string]bool{},
		containers: map[string]*fakeContainer{},
		execs:      map[string]*fakeExecState{},
	}
	server := &http.Server{Handler: fake}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
		_ = os.Remove(socket)
	})
	return NewDockerDriver(socket, filepath.Join(dir, "data")), fake
}

func (f *fakeDocker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery)
	f.mu.Unlock()

	requestPath := r.URL.Path
	switch {
	case requestPath == "/_ping":
		_, _ = io.WriteString(w, "OK")
	case requestPath == "/images/create":
		from := r.URL.Query().Get("fromImage")
		tag := r.URL.Query().Get("tag")
		f.mu.Lock()
		f.images[from+":"+tag] = true
		f.mu.Unlock()
		_, _ = io.WriteString(w, `{"status":"pulled"}`+"\n")
	case strings.HasPrefix(requestPath, "/images/") && strings.HasSuffix(requestPath, "/json"):
		name := strings.TrimSuffix(strings.TrimPrefix(requestPath, "/images/"), "/json")
		f.mu.Lock()
		found := f.images[name]
		f.mu.Unlock()
		if !found {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"message":"no such image"}`)
			return
		}
		_, _ = io.WriteString(w, `{}`)
	case requestPath == "/containers/create":
		f.handleCreate(w, r)
	case strings.HasPrefix(requestPath, "/containers/"):
		f.handleContainer(w, r)
	case strings.HasPrefix(requestPath, "/exec/"):
		f.handleExec(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeDocker) handleCreate(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	name := r.URL.Query().Get("name")
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.containers[name]; exists {
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"message":"name in use"}`)
		return
	}
	f.nextID++
	id := fmt.Sprintf("cid-%d", f.nextID)
	labels := map[string]string{}
	if raw, ok := body["Labels"].(map[string]any); ok {
		for key, value := range raw {
			labels[key] = fmt.Sprint(value)
		}
	}
	image, _ := body["Image"].(string)
	f.containers[name] = &fakeContainer{id: id, name: name, image: image, labels: labels}
	f.creates = append(f.creates, body)
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"Id": id})
}

func (f *fakeDocker) handleContainer(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/containers/")
	parts := strings.Split(rest, "/")

	f.mu.Lock()
	defer f.mu.Unlock()

	if r.Method == http.MethodDelete && len(parts) == 1 {
		if container := f.findLocked(parts[0]); container != nil {
			delete(f.containers, container.name)
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if len(parts) < 2 {
		http.NotFound(w, r)
		return
	}
	container := f.findLocked(parts[0])
	if container == nil {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"message":"no such container"}`)
		return
	}
	switch parts[1] {
	case "json":
		_ = json.NewEncoder(w).Encode(map[string]any{
			"Id":   container.id,
			"Name": "/" + container.name,
			"Config": map[string]any{
				"Image":  container.image,
				"Labels": container.labels,
			},
			"State": map[string]any{"Running": container.running},
		})
	case "start":
		container.running = true
		w.WriteHeader(http.StatusNoContent)
	case "stop":
		container.running = false
		w.WriteHeader(http.StatusNoContent)
	case "kill":
		container.running = false
		w.WriteHeader(http.StatusNoContent)
	case "exec":
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.nextID++
		id := fmt.Sprintf("exec-%d", f.nextID)
		state := &fakeExecState{}
		if raw, ok := body["Cmd"].([]any); ok {
			for _, item := range raw {
				state.cmd = append(state.cmd, fmt.Sprint(item))
			}
		}
		if raw, ok := body["Env"].([]any); ok {
			for _, item := range raw {
				state.env = append(state.env, fmt.Sprint(item))
			}
		}
		state.workdir, _ = body["WorkingDir"].(string)
		f.execs[id] = state
		f.execCreate = append(f.execCreate, body)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"Id": id})
	case "archive":
		f.handleArchive(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeDocker) handleArchive(w http.ResponseWriter, r *http.Request) {
	reader := tar.NewReader(r.Body)
	header, err := reader.Next()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.archives = append(f.archives, fakeArchive{
		path: r.URL.Query().Get("path"),
		name: header.Name,
		mode: header.Mode,
		data: data,
	})
	w.WriteHeader(http.StatusOK)
}

func (f *fakeDocker) handleExec(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/exec/")
	parts := strings.Split(rest, "/")
	f.mu.Lock()
	state := f.execs[parts[0]]
	f.mu.Unlock()
	if state == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if len(parts) == 2 && parts[1] == "json" {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"Running":  false,
			"ExitCode": state.exit,
		})
		return
	}
	if len(parts) == 2 && parts[1] == "start" {
		f.mu.Lock()
		hook := f.execHook
		f.mu.Unlock()
		stdout, stderr, exit := "", "", 0
		if hook != nil {
			stdout, stderr, exit = hook(state.cmd)
		} else {
			stdout, stderr, exit = f.defaultExec(state.cmd)
		}
		f.mu.Lock()
		state.stdout, state.stderr, state.exit = stdout, stderr, exit
		state.started = true
		f.mu.Unlock()

		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "hijack unsupported", http.StatusInternalServerError)
			return
		}
		conn, buffered, err := hijacker.Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.WriteString(buffered, "HTTP/1.1 101 UPGRADED\r\n"+
			"Content-Type: application/vnd.docker.raw-stream\r\n"+
			"Connection: Upgrade\r\nUpgrade: tcp\r\n\r\n")
		writeFrame(buffered, 1, []byte(stdout))
		writeFrame(buffered, 2, []byte(stderr))
		_ = buffered.Flush()
		return
	}
	http.NotFound(w, r)
}

func (f *fakeDocker) defaultExec(cmd []string) (string, string, int) {
	joined := strings.Join(cmd, " ")
	if strings.Contains(joined, "rev-parse") {
		f.mu.Lock()
		ready := f.gitReady
		f.mu.Unlock()
		if ready {
			return "true\n", "", 0
		}
		return "", "not a git repository", 1
	}
	if strings.Contains(joined, "clone") {
		f.mu.Lock()
		f.gitReady = true
		f.mu.Unlock()
		return "", "", 0
	}
	if strings.Contains(joined, "git config") {
		return "", "", 0
	}
	return `{"result":"ok"}`, "", 0
}

func (f *fakeDocker) findLocked(nameOrID string) *fakeContainer {
	if container, ok := f.containers[nameOrID]; ok {
		return container
	}
	for _, container := range f.containers {
		if container.id == nameOrID {
			return container
		}
	}
	return nil
}

func writeFrame(w io.Writer, stream byte, data []byte) {
	if len(data) == 0 {
		return
	}
	header := make([]byte, 8)
	header[0] = stream
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	_, _ = w.Write(header)
	_, _ = w.Write(data)
}

func (f *fakeDocker) hasRequestContains(fragment string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, request := range f.requests {
		if strings.Contains(request, fragment) {
			return true
		}
	}
	return false
}

func (f *fakeDocker) container(name string) *fakeContainer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.containers[name]
}

func (f *fakeDocker) setImage(name string) {
	f.mu.Lock()
	f.images[name] = true
	f.mu.Unlock()
}

func (f *fakeDocker) setExecHook(hook func(cmd []string) (stdout, stderr string, exit int)) {
	f.mu.Lock()
	f.execHook = hook
	f.mu.Unlock()
}

func (f *fakeDocker) createCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.creates)
}

func testSpec(gitRepo string) *WorkspaceSpec {
	spec := &WorkspaceSpec{
		Name:      "sbx",
		Driver:    "docker",
		Image:     "img@sha256:abc",
		Egress:    "none",
		Resources: map[string]any{"cpus": float64(2), "memory": "512m"},
	}
	if gitRepo != "" {
		spec.Provision = map[string]any{"git": map[string]any{"repo": gitRepo, "branch": "main"}}
	}
	return spec
}

func TestDockerEnsureCreatesAndAttaches(t *testing.T) {
	driver, fake := startFakeDocker(t)
	fake.setImage("img@sha256:abc")
	ctx := context.Background()

	handle, err := driver.Ensure(ctx, testSpec(""), "exec1", "ws1")
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if handle.Path != "/work" || handle.ID != "exec1:ws1" || handle.Key != "ws1" {
		t.Errorf("handle = %+v", handle)
	}
	datadir := handle.Data["datadir"]
	if info, err := os.Stat(datadir); err != nil || !info.IsDir() {
		t.Fatalf("datadir %s: %v", datadir, err)
	}
	meta := filepath.Join(driver.DataRoot, ".plaita-go", filepath.Base(datadir)+".json")
	if _, err := os.Stat(meta); err != nil {
		t.Errorf("metadata missing: %v", err)
	}

	fake.mu.Lock()
	creates := append([]map[string]any(nil), fake.creates...)
	fake.mu.Unlock()
	if len(creates) != 1 {
		t.Fatalf("creates = %d", len(creates))
	}
	create := creates[0]
	if create["Image"] != "img@sha256:abc" || create["WorkingDir"] != "/work" {
		t.Errorf("create = %v", create)
	}
	labels, _ := create["Labels"].(map[string]any)
	if labels[labelEngine] != engineName || labels[labelExecutionID] != "exec1" ||
		labels[labelWorkspace] != "ws1" {
		t.Errorf("labels = %v", labels)
	}
	hostConfig, _ := create["HostConfig"].(map[string]any)
	binds, _ := hostConfig["Binds"].([]any)
	if len(binds) != 1 || binds[0] != datadir+":/work" {
		t.Errorf("binds = %v", hostConfig)
	}
	if hostConfig["NetworkMode"] != "none" {
		t.Errorf("network = %v", hostConfig["NetworkMode"])
	}
	if hostConfig["NanoCpus"] != float64(2e9) {
		t.Errorf("nano cpus = %v", hostConfig["NanoCpus"])
	}
	if hostConfig["Memory"] != float64(512<<20) {
		t.Errorf("memory = %v", hostConfig["Memory"])
	}

	// Second ensure attaches (no new container).
	if _, err := driver.Ensure(ctx, testSpec(""), "exec1", "ws1"); err != nil {
		t.Fatalf("second Ensure: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.creates) != 1 {
		t.Errorf("ensure is not idempotent: creates = %d", len(fake.creates))
	}
}

func TestDockerEnsureForeignContainerFailsClosed(t *testing.T) {
	driver, fake := startFakeDocker(t)
	fake.setImage("img@sha256:abc")
	name := ResourceName("exec1", "ws1", "ctr")
	fake.mu.Lock()
	fake.containers[name] = &fakeContainer{
		id: "python-ctr", name: name, image: "img@sha256:abc",
		labels: map[string]string{}, running: true,
	}
	fake.mu.Unlock()
	if _, err := driver.Ensure(context.Background(), testSpec(""), "exec1", "ws1"); err == nil ||
		!strings.Contains(err.Error(), "collision") {
		t.Fatalf("err = %v, want a collision refusal", err)
	}
}

func TestDockerEnsureForeignDataDirFailsClosed(t *testing.T) {
	driver, fake := startFakeDocker(t)
	fake.setImage("img@sha256:abc")
	datadir := filepath.Join(driver.DataRoot, ResourceName("exec1", "ws1", "data"))
	if err := os.MkdirAll(datadir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(datadir, "foreign.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Ensure(context.Background(), testSpec(""), "exec1", "ws1"); err == nil ||
		!strings.Contains(err.Error(), "non-empty") {
		t.Fatalf("err = %v, want a foreign-data refusal", err)
	}
}

func TestDockerEnsureImageMismatchRecreates(t *testing.T) {
	driver, fake := startFakeDocker(t)
	fake.setImage("img@sha256:abc")
	ctx := context.Background()
	handle, err := driver.Ensure(ctx, testSpec(""), "exec1", "ws1")
	if err != nil {
		t.Fatal(err)
	}
	name := handle.Data["container"]
	fake.mu.Lock()
	fake.containers[name].image = "old@sha256:old"
	fake.mu.Unlock()
	if _, err := driver.Ensure(ctx, testSpec(""), "exec1", "ws1"); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if creates := fake.createCount(); creates != 2 {
		t.Errorf("creates = %d, want recreate", creates)
	}
	if !fake.hasRequestContains("/containers/" + name) {
		t.Errorf("no container lifecycle requests recorded")
	}
}

func TestDockerInjectAndExec(t *testing.T) {
	driver, fake := startFakeDocker(t)
	fake.setImage("img@sha256:abc")
	fake.setExecHook(func(cmd []string) (string, string, int) {
		if len(cmd) > 0 && cmd[len(cmd)-1] == "hi" {
			return "hello\n", "warn\n", 0
		}
		return "", "", 0
	})
	ctx := context.Background()
	handle, err := driver.Ensure(ctx, testSpec(""), "exec1", "ws1")
	if err != nil {
		t.Fatal(err)
	}

	if err := driver.InjectFile(ctx, handle, "/tmp/plaita.env", []byte("TOKEN=secret\n"), 0o600); err != nil {
		t.Fatalf("InjectFile: %v", err)
	}
	fake.mu.Lock()
	archives := append([]fakeArchive(nil), fake.archives...)
	fake.mu.Unlock()
	if len(archives) != 1 {
		t.Fatalf("archives = %d", len(archives))
	}
	if archives[0].name != "plaita.env" || archives[0].path != "/tmp" ||
		archives[0].mode != 0o600 || string(archives[0].data) != "TOKEN=secret\n" {
		t.Errorf("archive = %+v", archives[0])
	}

	result, err := driver.Exec(ctx, handle, ExecOptions{
		Cmd:     []string{"echo", "hi"},
		Env:     map[string]string{"PLAITA_EXECUTION_ID": "exec1"},
		WorkDir: "/work",
		EnvFile: "/tmp/plaita.env",
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if result.ExitCode != 0 || result.Stdout != "hello\n" || result.Stderr != "warn\n" {
		t.Errorf("result = %+v", result)
	}
	fake.mu.Lock()
	execCreate := append([]map[string]any(nil), fake.execCreate...)
	fake.mu.Unlock()
	if len(execCreate) != 1 {
		t.Fatalf("exec creates = %d", len(execCreate))
	}
	cmd, _ := execCreate[0]["Cmd"].([]any)
	if len(cmd) < 3 || cmd[0] != "/bin/sh" || cmd[1] != "-c" {
		t.Fatalf("wrapped cmd = %v", cmd)
	}
	script, _ := cmd[2].(string)
	if !strings.Contains(script, "export \"$line\"") || !strings.Contains(script, "'/tmp/plaita.env'") {
		t.Errorf("wrapper script = %q", script)
	}
	if execCreate[0]["WorkingDir"] != "/work" {
		t.Errorf("workdir = %v", execCreate[0]["WorkingDir"])
	}
	env, _ := execCreate[0]["Env"].([]any)
	if len(env) != 1 || env[0] != "PLAITA_EXECUTION_ID=exec1" {
		t.Errorf("exec env = %v", env)
	}

	fake.setExecHook(func([]string) (string, string, int) { return "", "failed", 3 })
	result, err = driver.Exec(ctx, handle, ExecOptions{Cmd: []string{"false"}})
	if err != nil {
		t.Fatalf("Exec non-zero: %v", err)
	}
	if result.ExitCode != 3 || result.Stderr != "failed" {
		t.Errorf("non-zero result = %+v", result)
	}
}

func TestDockerProvisionClone(t *testing.T) {
	driver, fake := startFakeDocker(t)
	fake.setImage("img@sha256:abc")
	handle, err := driver.Ensure(context.Background(), testSpec("https://example.com/r.git"), "exec1", "ws1")
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	fake.mu.Lock()
	var clone []string
	for _, body := range fake.execCreate {
		raw, _ := body["Cmd"].([]any)
		var cmd []string
		for _, item := range raw {
			cmd = append(cmd, fmt.Sprint(item))
		}
		if len(cmd) > 2 && cmd[0] == "git" && strings.Contains(strings.Join(cmd, " "), "clone") {
			clone = cmd
		}
	}
	execCreates := append([]map[string]any(nil), fake.execCreate...)
	fake.mu.Unlock()
	if clone == nil {
		t.Fatalf("no clone exec: %+v", execCreates)
	}
	joined := strings.Join(clone, " ")
	for _, want := range []string{"--branch main", "--single-branch", "https://example.com/r.git", "."} {
		if !strings.Contains(joined, want) {
			t.Errorf("clone = %v (missing %q)", clone, want)
		}
	}
	datadir := handle.Data["datadir"]
	if _, err := os.Stat(filepath.Join(datadir, ".plaita-sandbox.json")); err != nil {
		t.Errorf("sidecar missing: %v", err)
	}
}

func TestDockerImagePull(t *testing.T) {
	driver, fake := startFakeDocker(t)
	if _, err := driver.Ensure(context.Background(), testSpec(""), "exec1", "ws1"); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if !fake.hasRequestContains("/images/create") {
		t.Errorf("missing image was not pulled: %v", fake.requests)
	}
}

func TestDockerRelease(t *testing.T) {
	driver, fake := startFakeDocker(t)
	fake.setImage("img@sha256:abc")
	ctx := context.Background()
	handle, err := driver.Ensure(ctx, testSpec(""), "exec1", "ws1")
	if err != nil {
		t.Fatal(err)
	}
	datadir := handle.Data["datadir"]
	if err := driver.Release(ctx, handle, ReleaseOptions{KeepData: true, Failed: true, FailureReason: "boom"}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	marker := filepath.Join(datadir, ".plaita-sandbox-failed.json")
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("failure marker: %v", err)
	}
	if !strings.Contains(string(data), `"boom"`) {
		t.Errorf("marker = %s", data)
	}
	if fake.container(handle.Data["container"]) != nil {
		t.Errorf("container still present after release")
	}
	if !fake.hasRequestContains("/stop") || !strings.Contains(strings.Join(fake.requests, "\n"), "DELETE") {
		t.Errorf("stop/rm not called: %v", fake.requests)
	}

	// keep_data=false drops the data directory.
	handle2, err := driver.Ensure(ctx, testSpec(""), "exec2", "ws2")
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.Release(ctx, handle2, ReleaseOptions{KeepData: false}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := os.Stat(handle2.Data["datadir"]); !os.IsNotExist(err) {
		t.Errorf("data dir not removed: %v", err)
	}
}

func TestDockerExecTimeoutKillsContainer(t *testing.T) {
	driver, fake := startFakeDocker(t)
	fake.setImage("img@sha256:abc")
	fake.setExecHook(func([]string) (string, string, int) {
		time.Sleep(time.Second)
		return "", "", 0
	})
	ctx := context.Background()
	handle, err := driver.Ensure(ctx, testSpec(""), "exec1", "ws1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = driver.Exec(ctx, handle, ExecOptions{
		Cmd:     []string{"sleep", "10"},
		Timeout: 200 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("err = %v, want deadline", err)
	}
	deadline := time.Now().Add(time.Second)
	for !fake.hasRequestContains("/kill") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !fake.hasRequestContains("/kill") {
		t.Errorf("timeout did not kill the container: %v", fake.requests)
	}
}

// TestDockerLive is the real-daemon path, gated so CI and this machine (Docker
// stopped) stay green. PLAITA_SANDBOX_LIVE=1 PLAITA_SANDBOX_LIVE_IMAGE=alpine:3.20.
func TestDockerLive(t *testing.T) {
	if os.Getenv("PLAITA_SANDBOX_LIVE") != "1" {
		t.Skip("set PLAITA_SANDBOX_LIVE=1 to run against a real Docker daemon")
	}
	driver := NewDockerDriverFromEnv()
	if _, err := os.Stat(driver.Socket); err != nil {
		t.Skipf("docker socket unavailable: %v", err)
	}
	image := os.Getenv("PLAITA_SANDBOX_LIVE_IMAGE")
	if image == "" {
		image = "alpine:3.20"
	}
	spec := &WorkspaceSpec{
		Name:          "live",
		Driver:        "docker",
		Image:         image,
		AllowUnpinned: true,
		Egress:        "none",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	handle, err := driver.Ensure(ctx, spec, "live-exec", "live-ws")
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	defer func() { _ = driver.Release(context.Background(), handle, ReleaseOptions{KeepData: false}) }()

	envPath := "/tmp/plaita-live.env"
	if err := driver.InjectFile(ctx, handle, envPath, []byte("LIVE_TOKEN=abc\n"), 0o600); err != nil {
		t.Fatalf("InjectFile: %v", err)
	}
	result, err := driver.Exec(ctx, handle, ExecOptions{
		Cmd:     []string{"sh", "-c", "printf '%s' \"$LIVE_TOKEN\""},
		WorkDir: "/work",
		EnvFile: envPath,
		Timeout: time.Minute,
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if result.ExitCode != 0 || result.Stdout != "abc" {
		t.Fatalf("result = %+v", result)
	}
}
