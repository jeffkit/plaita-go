// Package shadow implements the M2 reconciliation harness: it executes the
// same deployment-form flow in the plaita Python engine and in this Go engine
// and compares the final output plus the per-node trace.
//
// Comparison semantics (see docs/SHADOW_REPORT.md):
//   - values are compared in the JSON domain, so 1 and 1.0 are numerically
//     equal (declared divergence: int/float rendering) and map key order is
//     irrelevant (declared divergence: key ordering);
//   - differences are reported field by field with a JSON path;
//   - node traces are grouped per flow_id and compared in order inside each
//     group, which keeps parallel branch interleaving out of the comparison.
package shadow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
)

// Difference is one field-level divergence between the Python reference run
// and the Go run.
type Difference struct {
	Path      string `json:"path"`
	Kind      string `json:"kind"`
	Reference any    `json:"reference,omitempty"`
	Actual    any    `json:"actual,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

// Difference kinds.
const (
	DiffTypeMismatch   = "type_mismatch"
	DiffValueMismatch  = "value_mismatch"
	DiffMissing        = "missing"
	DiffUnexpected     = "unexpected"
	DiffLength         = "length_mismatch"
	DiffTraceFlowMiss  = "trace_flow_missing"
	DiffTraceFlowExtra = "trace_flow_unexpected"
	DiffTraceLength    = "trace_length_mismatch"
	DiffNodeID         = "node_id_mismatch"
	DiffNodeType       = "node_type_mismatch"
	DiffNodeError      = "node_error_mismatch"
	DiffExecution      = "execution_mismatch"
)

// canonical converts an arbitrary Go value into the JSON value domain
// (nil/bool/string/json.Number/[]any/map[string]any). This normalizes typed
// slices/maps and numeric Go types so both engines are compared on equal
// footing. Non-JSON-marshalable values are returned as their fmt form with
// ok=false so the caller can report an explicit difference instead of a panic.
func canonical(v any) (any, bool) {
	if v == nil {
		return nil, true
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("<unserializable %T: %v>", v, v), false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return fmt.Sprintf("<unserializable %T: %v>", v, v), false
	}
	return out, true
}

// CompareValues returns the field-level differences between the reference
// value (Python engine output) and the actual value (Go engine output).
func CompareValues(reference, actual any) []Difference {
	ref, okRef := canonical(reference)
	act, okAct := canonical(actual)
	var diffs []Difference
	if !okRef || !okAct {
		diffs = append(diffs, Difference{
			Path:      "$",
			Kind:      DiffValueMismatch,
			Reference: ref,
			Actual:    act,
			Detail:    "value is not JSON-serializable; compared by string form",
		})
	}
	compareValue("$", ref, act, &diffs)
	return diffs
}

func compareValue(path string, ref, act any, diffs *[]Difference) {
	if ref == nil || act == nil {
		if ref == nil && act == nil {
			return
		}
		*diffs = append(*diffs, Difference{
			Path: path, Kind: DiffTypeMismatch,
			Reference: ref, Actual: act,
			Detail: fmt.Sprintf("type %s vs %s", typeName(ref), typeName(act)),
		})
		return
	}

	if refNum, ok := numericRat(ref); ok {
		if actNum, ok := numericRat(act); ok {
			if refNum.Cmp(actNum) != 0 {
				*diffs = append(*diffs, Difference{
					Path: path, Kind: DiffValueMismatch,
					Reference: ref, Actual: act,
					Detail: fmt.Sprintf("numeric %s != %s", refNum.RatString(), actNum.RatString()),
				})
			}
			return
		}
	}

	switch refVal := ref.(type) {
	case bool:
		actVal, ok := act.(bool)
		if !ok {
			*diffs = append(*diffs, typeDiff(path, ref, act))
			return
		}
		if refVal != actVal {
			*diffs = append(*diffs, valueDiff(path, ref, act))
		}
	case string:
		actVal, ok := act.(string)
		if !ok {
			*diffs = append(*diffs, typeDiff(path, ref, act))
			return
		}
		if refVal != actVal {
			*diffs = append(*diffs, valueDiff(path, ref, act))
		}
	case []any:
		actVal, ok := act.([]any)
		if !ok {
			*diffs = append(*diffs, typeDiff(path, ref, act))
			return
		}
		if len(refVal) != len(actVal) {
			*diffs = append(*diffs, Difference{
				Path: path, Kind: DiffLength,
				Reference: len(refVal), Actual: len(actVal),
				Detail: fmt.Sprintf("array length %d vs %d", len(refVal), len(actVal)),
			})
			return
		}
		for i := range refVal {
			compareValue(fmt.Sprintf("%s[%d]", path, i), refVal[i], actVal[i], diffs)
		}
	case map[string]any:
		actVal, ok := act.(map[string]any)
		if !ok {
			*diffs = append(*diffs, typeDiff(path, ref, act))
			return
		}
		keys := make([]string, 0, len(refVal))
		for k := range refVal {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			actItem, found := actVal[k]
			if !found {
				*diffs = append(*diffs, Difference{
					Path: childPath(path, k), Kind: DiffMissing,
					Reference: refVal[k],
					Detail:    "key present in python output but missing in go output",
				})
				continue
			}
			compareValue(childPath(path, k), refVal[k], actItem, diffs)
		}
		extra := make([]string, 0)
		for k := range actVal {
			if _, found := refVal[k]; !found {
				extra = append(extra, k)
			}
		}
		sort.Strings(extra)
		for _, k := range extra {
			*diffs = append(*diffs, Difference{
				Path: childPath(path, k), Kind: DiffUnexpected,
				Actual: actVal[k],
				Detail: "key present in go output but missing in python output",
			})
		}
	default:
		if fmt.Sprint(ref) != fmt.Sprint(act) {
			*diffs = append(*diffs, valueDiff(path, ref, act))
		}
	}
}

func valueDiff(path string, ref, act any) Difference {
	return Difference{
		Path: path, Kind: DiffValueMismatch,
		Reference: ref, Actual: act,
		Detail: fmt.Sprintf("%s vs %s", compactJSON(ref), compactJSON(act)),
	}
}

func typeDiff(path string, ref, act any) Difference {
	return Difference{
		Path: path, Kind: DiffTypeMismatch,
		Reference: ref, Actual: act,
		Detail: fmt.Sprintf("type %s vs %s", typeName(ref), typeName(act)),
	}
}

func childPath(parent, key string) string {
	return parent + "." + key
}

func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case string:
		return "string"
	case json.Number, float64, float32, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return "number"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return fmt.Sprintf("%T", v)
	}
}

// numericRat converts a JSON-domain numeric value into an exact rational.
// Using big.Rat keeps int/float equivalence exact (1 == 1.0) and avoids the
// float64 precision trap for large integers.
func numericRat(v any) (*big.Rat, bool) {
	switch t := v.(type) {
	case json.Number:
		r, ok := new(big.Rat).SetString(t.String())
		return r, ok
	case int:
		return new(big.Rat).SetInt64(int64(t)), true
	case int8:
		return new(big.Rat).SetInt64(int64(t)), true
	case int16:
		return new(big.Rat).SetInt64(int64(t)), true
	case int32:
		return new(big.Rat).SetInt64(int64(t)), true
	case int64:
		return new(big.Rat).SetInt64(t), true
	case uint:
		return new(big.Rat).SetUint64(uint64(t)), true
	case uint8:
		return new(big.Rat).SetUint64(uint64(t)), true
	case uint16:
		return new(big.Rat).SetUint64(uint64(t)), true
	case uint32:
		return new(big.Rat).SetUint64(uint64(t)), true
	case uint64:
		return new(big.Rat).SetUint64(t), true
	case float32:
		r := new(big.Rat).SetFloat64(float64(t))
		return r, r != nil
	case float64:
		r := new(big.Rat).SetFloat64(t)
		return r, r != nil
	default:
		return nil, false
	}
}

func compactJSON(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(raw)
}
