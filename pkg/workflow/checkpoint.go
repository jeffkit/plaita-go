package workflow

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
)

// Execution checkpoint (M6, checkpoint v1.5).
//
// A checkpoint is the complete, resumable snapshot taken at a suspend
// boundary: the flow's expression roots ($INPUT/$NODE/$GLOBAL/$ENV/$ERROR/
// $PARENT), the last-node pointer (the suspended node) and the metadata the
// worker needs to write the suspended state row. The schema mirrors plaita's
// checkpoint dict (`plaita/core/state.py`, `ExecutionContext.to_dict`) with
// the Python prefix folded into the keys ($NODE, $LAST_NODE, ...), because
// that is what the expression engine consumes.

const (
	// CheckpointSchemaVersion is the snapshot envelope version this build
	// writes. Loaders reject unknown/newer versions (fail-closed).
	CheckpointSchemaVersion = 1

	// SnapshotMaxDepth is the defensive nesting ceiling for one value
	// (plaita `_SNAPSHOT_MAX_DEPTH`, 10_000). A deeper structure is a
	// pathological payload, not a real flow value; failing with the exact
	// depth and path keeps the corruption observable at the snapshot exit.
	SnapshotMaxDepth = 10_000

	// snapshotCycleMark truncates a container cycle with its path. A string
	// is JSON-expressible, so the checkpoint can still be written; keeping
	// the reference instead would fail at the host serializer with no path
	// information.
	snapshotCycleMark = "<recursive: %s>"

	snapshotPathHead = 4
	snapshotPathTail = 2

	// maxParentChainDepth bounds the $PARENT snapshot recursion; execution
	// context chains are short (child/loop/parallel nesting), anything deeper
	// is a programming error.
	maxParentChainDepth = 64
)

// StateSnapshotError mirrors plaita's StateSnapshotError: a value cannot be
// represented in a checkpoint (depth ceiling, non-string map key, ...). It is
// deterministic and must never be retried.
type StateSnapshotError struct{ Message string }

func (e *StateSnapshotError) Error() string { return e.Message }

func snapshotErrorf(format string, args ...any) error {
	return &StateSnapshotError{Message: fmt.Sprintf(format, args...)}
}

// Checkpoint kinds (#32): suspend boundaries (M6) and per-step positions
// share the checkpoint store; the kind tells the resume entries apart. A
// checkpoint written before the kind field existed has an empty kind —
// readers treat that as a suspend boundary unless its resume_type says
// otherwise (worker legacy fallback).
const (
	CheckpointKindSuspend = "suspend"
	CheckpointKindPerStep = "per-step"
)

// Checkpoint is one resumable suspend boundary.
type Checkpoint struct {
	SchemaVersion int `json:"schema_version"`
	// Kind discriminates suspend boundaries from per-step positions
	// (CheckpointKind*); empty means a legacy suspend snapshot.
	Kind          string         `json:"kind,omitempty"`
	ExecutionID   string         `json:"execution_id,omitempty"`
	TenantID      string         `json:"tenant_id,omitempty"`
	FlowID        string         `json:"flow_id,omitempty"`
	SuspendedNode NodeID         `json:"suspended_node"`
	NextNode      NodeID         `json:"next_node,omitempty"`
	ResumeType    string         `json:"resume_type,omitempty"`
	Context       map[string]any `json:"context"`
	CreatedAt     string         `json:"created_at"`
}

// CheckpointStore persists checkpoints. LoadCheckpoint reports a missing
// checkpoint as (nil, nil) — the worker's read-side contract (a real Redis
// failure is an error, never silently "missing").
type CheckpointStore interface {
	SaveCheckpoint(ctx context.Context, cp *Checkpoint) error
	LoadCheckpoint(ctx context.Context, tenantID, executionID string) (*Checkpoint, error)
	DeleteCheckpoint(ctx context.Context, tenantID, executionID string) error
}

// MemoryCheckpointStore is the in-process store used by engine tests and by
// hosts that run a single execution in memory.
type MemoryCheckpointStore struct {
	mu    sync.Mutex
	items map[string]*Checkpoint
}

// NewMemoryCheckpointStore returns an empty in-memory checkpoint store.
func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{items: map[string]*Checkpoint{}}
}

func (s *MemoryCheckpointStore) key(tenantID, executionID string) string {
	return tenantID + "\x00" + executionID
}

// SaveCheckpoint stores (a copy of) the checkpoint.
func (s *MemoryCheckpointStore) SaveCheckpoint(_ context.Context, cp *Checkpoint) error {
	if cp == nil {
		return snapshotErrorf("checkpoint is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	clone := *cp
	s.items[s.key(cp.TenantID, cp.ExecutionID)] = &clone
	return nil
}

// LoadCheckpoint returns the stored checkpoint or (nil, nil) when absent.
func (s *MemoryCheckpointStore) LoadCheckpoint(_ context.Context, tenantID, executionID string) (*Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp, ok := s.items[s.key(tenantID, executionID)]
	if !ok {
		return nil, nil
	}
	clone := *cp
	return &clone, nil
}

// DeleteCheckpoint removes a checkpoint (idempotent).
func (s *MemoryCheckpointStore) DeleteCheckpoint(_ context.Context, tenantID, executionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, s.key(tenantID, executionID))
	return nil
}

// SnapshotValue materialises v into a fresh, JSON-safe snapshot, mirroring
// plaita's `_snapshot_value`:
//
//   - maps (string keys) and slices/arrays are rebuilt deeply into
//     map[string]any / []any; the result never aliases the source containers;
//   - a container already on the current DFS path becomes
//     "<recursive: $['a'][0]>" — cycles are truncated explicitly, never left
//     to explode (or silently corrupt) the host JSON serializer;
//   - a DAG (the same child referenced by two sibling keys) is NOT a cycle
//     and is copied per reference, like Python's path-only `seen_ids`;
//   - a value more than SnapshotMaxDepth containers deep raises
//     *StateSnapshotError carrying the depth and path;
//   - scalars and non-container host values pass through unchanged (the
//     snapshot rebuild面 is exactly maps/slices, the Go encoding of Python's
//     dict/list/tuple rebuild set).
//
// The traversal is an explicit stack, not recursion: data depth never
// consumes Go stack frames, matching the #107 fix in plaita.
func SnapshotValue(root any) (any, error) {
	c, isContainer, err := asSnapshotContainer(root)
	if err != nil {
		return nil, err
	}
	if !isContainer {
		return root, nil
	}
	mapDst, sliceDst := newSnapshotContainer(c)
	rootFrame := newSnapshotFrame(c, mapDst, sliceDst, nil)
	stack := []*snapshotFrame{rootFrame}
	seen := map[snapshotIdentity]bool{}
	if c.ptr != 0 {
		seen[c.identity()] = true
	}

	for len(stack) > 0 {
		f := stack[len(stack)-1]
		if f.i >= f.n {
			if f.ptr != 0 {
				delete(seen, snapshotIdentity{ptr: f.ptr, length: f.length})
			}
			stack = stack[:len(stack)-1]
			continue
		}

		var key any
		var child any
		if f.kind == snapshotKindMap {
			k := f.keys[f.i]
			f.i++
			child = f.src.MapIndex(k).Interface()
			key, err = snapshotMapKey(k)
			if err != nil {
				return nil, err
			}
		} else {
			idx := f.i
			f.i++
			child = f.src.Index(idx).Interface()
			key = idx
		}

		childContainer, childIsContainer, err := asSnapshotContainer(child)
		if err != nil {
			return nil, err
		}
		if !childIsContainer {
			f.assign(key, child)
			continue
		}

		// len(stack) is the depth of the container currently being walked;
		// the child would live one level below it (plaita's `depth =
		// len(frames)` check before pushing).
		if depth := len(stack); depth > SnapshotMaxDepth {
			return nil, snapshotErrorf(
				"state snapshot depth %d exceeds limit %d at path %s",
				depth, SnapshotMaxDepth, snapshotPath(stack, key))
		}
		if childContainer.ptr != 0 && seen[childContainer.identity()] {
			f.assign(key, fmt.Sprintf(snapshotCycleMark, snapshotPath(stack, key)))
			continue
		}

		childMap, childSlice := newSnapshotContainer(childContainer)
		f.assign(key, snapshotContainerValue(childContainer, childMap, childSlice))
		if childContainer.ptr != 0 {
			seen[childContainer.identity()] = true
		}
		stack = append(stack, newSnapshotFrame(childContainer, childMap, childSlice, key))
	}

	return snapshotContainerValue(c, mapDst, sliceDst), nil
}

// SnapshotRuntimeContext snapshots the live runtime context into the
// checkpoint dict. opts (when non-nil) supplies $FLOW_ID/$EXECUTION_ID.
func SnapshotRuntimeContext(ctx context.Context, opts *SuspendOptions) (map[string]any, error) {
	rc := runtime.WorkflowRuntimeContext(ctx)
	return snapshotRuntimeContext(rc, opts, 0)
}

func snapshotRuntimeContext(rc *runtime.Context, opts *SuspendOptions, parentDepth int) (map[string]any, error) {
	if rc == nil {
		rc = runtime.NewContext()
	}
	out := map[string]any{}
	put := func(key string, value any) error {
		snap, err := SnapshotValue(value)
		if err != nil {
			return fmt.Errorf("state snapshot %s: %w", key, err)
		}
		out[key] = snap
		return nil
	}
	if err := put("$INPUT", rc.WorkflowInput); err != nil {
		return nil, err
	}
	if err := put("$NODE", rc.ExecuteNodeResults); err != nil {
		return nil, err
	}
	if err := put("$GLOBAL", rc.GlobalVariables); err != nil {
		return nil, err
	}
	if err := put("$ENV", rc.EnvVariables); err != nil {
		return nil, err
	}
	if len(rc.ExecuteNodeErrors) > 0 {
		if err := put("$ERROR", rc.ExecuteNodeErrors); err != nil {
			return nil, err
		}
	}
	if len(rc.CurrentNodeContext) > 0 {
		if err := put("$CURRENT", rc.CurrentNodeContext); err != nil {
			return nil, err
		}
	}
	out["$LAST_NODE"] = string(rc.LastNodeID)
	// plaita CheckpointSchema.SYSTEM_KEY_NAMES keeps BRANCH as system state:
	// the resumed run must see the branch taken before the suspend point.
	if rc.LastBranch != nil {
		out["$BRANCH"] = rc.LastBranch
	}
	// #11 system roots travel with every snapshot: the resumed run must see
	// the same $FLOW_ID/$EXECUTION_ID the original run had. opts (the host's
	// suspend wiring) keeps precedence for hosts that set them without
	// seeding the runtime context.
	flowID, executionID := rc.FlowID, rc.ExecutionID
	if opts != nil {
		if opts.FlowID != "" {
			flowID = opts.FlowID
		}
		if opts.ExecutionID != "" {
			executionID = opts.ExecutionID
		}
	}
	out["$FLOW_ID"] = flowID
	out["$EXECUTION_ID"] = executionID

	if rc.ParentWorkflowContext != nil {
		if parentDepth >= maxParentChainDepth {
			return nil, snapshotErrorf("state snapshot $PARENT chain exceeds %d levels", maxParentChainDepth)
		}
		if parentRC, ok := runtime.LookupWorkflowRuntimeContext(rc.ParentWorkflowContext); ok && parentRC != nil {
			parentSnap, err := snapshotRuntimeContext(parentRC, nil, parentDepth+1)
			if err != nil {
				return nil, err
			}
			out["$PARENT"] = parentSnap
		}
	}
	return out, nil
}

// RestoreRuntimeContext rebuilds a runtime context from a checkpoint dict and
// attaches it to ctx. The restored maps are the checkpoint's own snapped
// values (already private), so no live alias can reach the resumed run.
func RestoreRuntimeContext(ctx context.Context, checkpointContext map[string]any) (context.Context, error) {
	if checkpointContext == nil {
		return nil, snapshotErrorf("checkpoint context is missing")
	}
	rc := runtime.NewContext()
	rc.WorkflowInput = checkpointContext["$INPUT"]
	rc.ExecuteNodeResults = stringAnyMap(checkpointContext["$NODE"])
	rc.GlobalVariables = stringAnyMap(checkpointContext["$GLOBAL"])
	rc.EnvVariables = stringAnyMap(checkpointContext["$ENV"])
	rc.ExecuteNodeErrors = stringAnyMap(checkpointContext["$ERROR"])
	rc.CurrentNodeContext = stringAnyMap(checkpointContext["$CURRENT"])
	if lastNode, ok := checkpointContext["$LAST_NODE"].(string); ok {
		rc.LastNodeID = NodeID(lastNode)
	}
	if branch, ok := checkpointContext["$BRANCH"]; ok {
		rc.LastBranch = branch
	}
	if flowID, ok := checkpointContext["$FLOW_ID"].(string); ok {
		rc.FlowID = flowID
	}
	if executionID, ok := checkpointContext["$EXECUTION_ID"].(string); ok {
		rc.ExecutionID = executionID
	}
	if parent, ok := checkpointContext["$PARENT"].(map[string]any); ok {
		parentRC := restoreRuntimeChain(parent)
		rc.ParentWorkflowContext = runtime.WithWorkflowRuntimeContext(context.Background(), parentRC)
	}
	return runtime.WithWorkflowRuntimeContext(ctx, rc), nil
}

func restoreRuntimeChain(snap map[string]any) *runtime.Context {
	rc := runtime.NewContext()
	rc.WorkflowInput = snap["$INPUT"]
	rc.ExecuteNodeResults = stringAnyMap(snap["$NODE"])
	rc.GlobalVariables = stringAnyMap(snap["$GLOBAL"])
	rc.EnvVariables = stringAnyMap(snap["$ENV"])
	rc.ExecuteNodeErrors = stringAnyMap(snap["$ERROR"])
	rc.CurrentNodeContext = stringAnyMap(snap["$CURRENT"])
	if lastNode, ok := snap["$LAST_NODE"].(string); ok {
		rc.LastNodeID = NodeID(lastNode)
	}
	if branch, ok := snap["$BRANCH"]; ok {
		rc.LastBranch = branch
	}
	if flowID, ok := snap["$FLOW_ID"].(string); ok {
		rc.FlowID = flowID
	}
	if executionID, ok := snap["$EXECUTION_ID"].(string); ok {
		rc.ExecutionID = executionID
	}
	if parent, ok := snap["$PARENT"].(map[string]any); ok {
		rc.ParentWorkflowContext = runtime.WithWorkflowRuntimeContext(context.Background(), restoreRuntimeChain(parent))
	}
	return rc
}

func stringAnyMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

const (
	snapshotKindMap byte = iota
	snapshotKindSlice
	snapshotKindArray
)

type snapshotContainer struct {
	src    reflect.Value
	kind   byte
	ptr    uintptr // identity of the live container (0 = not shared)
	length int
}

// snapshotIdentity is the cycle-detection key. It includes the length because
// two slices can share a backing array at the same start pointer while being
// different values (s[0:1] vs s[0:2]); only an identical pointer AND length is
// treated as the same container on the current DFS path.
type snapshotIdentity struct {
	ptr    uintptr
	length int
}

func (c snapshotContainer) identity() snapshotIdentity {
	return snapshotIdentity{ptr: c.ptr, length: c.length}
}

// asSnapshotContainer classifies a value as a rebuildable container. Nil
// maps/slices stay nil (JSON null) instead of becoming an empty container —
// the snapshot must not invent a value the flow did not have. []byte is a
// scalar (its JSON encoding is base64, not an element array).
func asSnapshotContainer(v any) (snapshotContainer, bool, error) {
	if v == nil {
		return snapshotContainer{}, false, nil
	}
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Interface || rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return snapshotContainer{}, false, nil
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Map:
		if rv.IsNil() {
			return snapshotContainer{}, false, nil
		}
		if rv.Type().Key().Kind() != reflect.String && rv.Type().Key().Kind() != reflect.Interface {
			return snapshotContainer{}, false, snapshotErrorf(
				"state snapshot cannot represent map with %s keys: checkpoint keys must be strings",
				rv.Type().Key())
		}
		return snapshotContainer{src: rv, kind: snapshotKindMap, ptr: rv.Pointer()}, true, nil
	case reflect.Slice:
		if rv.IsNil() {
			return snapshotContainer{}, false, nil
		}
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return snapshotContainer{}, false, nil
		}
		return snapshotContainer{src: rv, kind: snapshotKindSlice, ptr: rv.Pointer(), length: rv.Len()}, true, nil
	case reflect.Array:
		return snapshotContainer{src: rv, kind: snapshotKindArray, length: rv.Len()}, true, nil
	}
	return snapshotContainer{}, false, nil
}

func newSnapshotContainer(c snapshotContainer) (map[string]any, []any) {
	if c.kind == snapshotKindMap {
		return make(map[string]any, c.src.Len()), nil
	}
	return nil, make([]any, c.src.Len())
}

func snapshotContainerValue(c snapshotContainer, m map[string]any, l []any) any {
	if c.kind == snapshotKindMap {
		return m
	}
	return l
}

// snapshotMapKey mirrors what JSON can express as an object key: strings
// pass through, Python-style scalar keys are stringified (json.dumps would
// do the same). Anything else is a named snapshot error, never a lossy
// silent conversion.
func snapshotMapKey(k reflect.Value) (string, error) {
	for k.Kind() == reflect.Interface || k.Kind() == reflect.Pointer {
		if k.IsNil() {
			return "", snapshotErrorf("state snapshot cannot represent nil map key")
		}
		k = k.Elem()
	}
	switch k.Kind() {
	case reflect.String:
		return k.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(k.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(k.Uint(), 10), nil
	case reflect.Bool:
		return strconv.FormatBool(k.Bool()), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(k.Float(), 'g', -1, 64), nil
	}
	return "", snapshotErrorf("state snapshot cannot represent %s as a map key", k.Type())
}

type snapshotFrame struct {
	src      reflect.Value
	kind     byte
	mapDst   map[string]any
	sliceDst []any
	keys     []reflect.Value // map keys, snapshotted once
	n        int
	i        int
	ptr      uintptr
	length   int
	segment  any
}

func newSnapshotFrame(c snapshotContainer, m map[string]any, l []any, segment any) *snapshotFrame {
	f := &snapshotFrame{
		src:      c.src,
		kind:     c.kind,
		mapDst:   m,
		sliceDst: l,
		n:        c.src.Len(),
		ptr:      c.ptr,
		length:   c.length,
		segment:  segment,
	}
	if c.kind == snapshotKindMap {
		f.keys = c.src.MapKeys()
	}
	return f
}

func (f *snapshotFrame) assign(key, value any) {
	if f.kind == snapshotKindMap {
		f.mapDst[key.(string)] = value
		return
	}
	f.sliceDst[key.(int)] = value
}

// snapshotPath renders the iterated stack as "$['a'][0]['b']" for error
// messages and cycle placeholders. Long paths are truncated in the middle
// (plaita `_snapshot_path`).
func snapshotPath(stack []*snapshotFrame, key any) string {
	segments := make([]any, 0, len(stack))
	for _, f := range stack[1:] {
		segments = append(segments, f.segment)
	}
	segments = append(segments, key)
	if len(segments) > snapshotPathHead+snapshotPathTail+1 {
		trimmed := make([]any, 0, snapshotPathHead+snapshotPathTail+1)
		trimmed = append(trimmed, segments[:snapshotPathHead]...)
		trimmed = append(trimmed, "...")
		trimmed = append(trimmed, segments[len(segments)-snapshotPathTail:]...)
		segments = trimmed
	}
	var b strings.Builder
	b.WriteString("$")
	for _, segment := range segments {
		b.WriteString("[")
		b.WriteString(reprPathSegment(segment))
		b.WriteString("]")
	}
	return b.String()
}

func reprPathSegment(segment any) string {
	if s, ok := segment.(string); ok {
		return "'" + strings.ReplaceAll(s, "'", `\'`) + "'"
	}
	return fmt.Sprint(segment)
}

// isoNow is the checkpoint timestamp format: the naive local wall clock the
// Python side writes (datetime.now().isoformat()).
func isoNow() string { return time.Now().Format("2006-01-02T15:04:05.000000") }
