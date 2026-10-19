package plaitaexpr

// pymod.go implements the printf-style branch of `$F.mod(a, b)` — Python's
// `a % b` when a is a string (expression.py _fn_mod, "数值 % 保持不动" — the
// numeric branch lives in math_functions.go).
//
// Supported surface (mirrors CPython str.__mod__ for the expression value
// domain): conversion types s r a d i u o x X e E f F g G c %, flags `-+ 0#`,
// width, precision, `%(key)s` mapping selection and list-as-tuple arguments.
// Go's fmt verbs are compatible for the float conversions; integer and string
// conversions are rendered manually to keep Python semantics (%d truncates
// floats toward zero, %x keeps a sign, %s goes through Python str()).

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

type pyFmtSpec struct {
	key       string // %(key)s mapping key ("" when positional)
	flagMinus bool
	flagPlus  bool
	flagSpace bool
	flagZero  bool
	flagAlt   bool
	width     int
	precision int
	hasPrec   bool
	conv      byte
}

// pyFormat renders the Python "%-format" of fmt with args (b). args is either
// a single value, a list (tuple) or a map (%(key)s selection — native Python
// requires the mapping BARE for keyed specs; a 1-tuple of a mapping raises
// "format requires a mapping"). It returns an error where Python would raise
// TypeError/KeyError/ValueError.
func pyFormat(fmtStr string, args any) (string, error) {
	argList, isTuple := pyFmtPositional(args)
	pos := 0
	mappingUsed := false
	nextArg := func(spec pyFmtSpec) (any, error) {
		if spec.key != "" {
			m, ok := args.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("$F.mod: format requires a mapping")
			}
			mappingUsed = true
			v, ok := m[spec.key]
			if !ok {
				return nil, fmt.Errorf("$F.mod: KeyError: %q", spec.key)
			}
			return v, nil
		}
		if pos >= len(argList) {
			return nil, fmt.Errorf("$F.mod: not enough arguments for format string")
		}
		v := argList[pos]
		pos++
		return v, nil
	}

	consumedAny := false
	var b strings.Builder
	i := 0
	for i < len(fmtStr) {
		c := fmtStr[i]
		if c != '%' {
			b.WriteByte(c)
			i++
			continue
		}
		spec, next, err := parsePyFmtSpec(fmtStr, i)
		if err != nil {
			return "", err
		}
		i = next
		if spec.conv == '%' {
			b.WriteByte('%')
			continue
		}
		arg, err := nextArg(spec)
		if err != nil {
			return "", err
		}
		consumedAny = true
		rendered, err := renderPyFmtSpec(spec, arg)
		if err != nil {
			return "", err
		}
		b.WriteString(rendered)
	}
	// Python's leftover-argument rule: a tuple with unconsumed members and a
	// single value that no conversion consumed both raise "not all arguments
	// converted"; an EMPTY tuple with no conversions is fine.
	switch {
	case isTuple:
		if pos < len(argList) {
			return "", fmt.Errorf("$F.mod: not all arguments converted during string formatting")
		}
	case !consumedAny && !mappingUsed:
		return "", fmt.Errorf("$F.mod: not all arguments converted during string formatting")
	}
	return b.String(), nil
}

// pyFmtPositional classifies the argument for positional conversions: a list
// is a tuple, any other value (a map included — Python treats a bare mapping
// as a single %s value) is a one-element tuple.
func pyFmtPositional(args any) ([]any, bool) {
	if l, ok := args.([]any); ok {
		return l, true
	}
	return []any{args}, false
}

// parsePyFmtSpec parses one % spec starting at fmt[i] == '%'. It returns the
// spec and the index just past it.
func parsePyFmtSpec(fmtStr string, i int) (pyFmtSpec, int, error) {
	spec := pyFmtSpec{width: -1, precision: -1}
	j := i + 1
	if j >= len(fmtStr) {
		return spec, j, fmt.Errorf("$F.mod: incomplete format")
	}
	// %(key)s mapping key
	if fmtStr[j] == '(' {
		end := strings.IndexByte(fmtStr[j:], ')')
		if end < 0 {
			return spec, j, fmt.Errorf("$F.mod: incomplete format key")
		}
		spec.key = fmtStr[j+1 : j+end]
		j += end + 1
		if j >= len(fmtStr) {
			return spec, j, fmt.Errorf("$F.mod: incomplete format")
		}
	}
	for j < len(fmtStr) {
		switch fmtStr[j] {
		case '-':
			spec.flagMinus = true
		case '+':
			spec.flagPlus = true
		case ' ':
			spec.flagSpace = true
		case '0':
			spec.flagZero = true
		case '#':
			spec.flagAlt = true
		default:
			goto flagsDone
		}
		j++
	}
flagsDone:
	if j < len(fmtStr) && fmtStr[j] == '*' {
		return spec, j + 1, fmt.Errorf("$F.mod: '*' width/precision is not supported")
	}
	for j < len(fmtStr) && fmtStr[j] >= '0' && fmtStr[j] <= '9' {
		if spec.width < 0 {
			spec.width = 0
		}
		spec.width = spec.width*10 + int(fmtStr[j]-'0')
		j++
	}
	if j < len(fmtStr) && fmtStr[j] == '.' {
		spec.hasPrec = true
		spec.precision = 0
		j++
		for j < len(fmtStr) && fmtStr[j] >= '0' && fmtStr[j] <= '9' {
			spec.precision = spec.precision*10 + int(fmtStr[j]-'0')
			j++
		}
	}
	if j >= len(fmtStr) {
		return spec, j, fmt.Errorf("$F.mod: incomplete format")
	}
	spec.conv = fmtStr[j]
	if !strings.ContainsRune("sradioxXeEfFgGc%", rune(spec.conv)) {
		return spec, j + 1, fmt.Errorf("$F.mod: unsupported format character %q", string(spec.conv))
	}
	return spec, j + 1, nil
}

// renderPyFmtSpec converts one argument according to spec and applies
// width/flags padding.
func renderPyFmtSpec(spec pyFmtSpec, arg any) (string, error) {
	switch spec.conv {
	case 's':
		return padPyFmt(spec, PyStr(arg)), nil
	case 'r', 'a':
		return padPyFmt(spec, PyRepr(arg)), nil
	case 'd', 'i', 'u':
		n, err := pyFmtInt(spec, arg)
		if err != nil {
			return "", err
		}
		s := strconv.FormatInt(n, 10)
		if spec.flagPlus && n >= 0 {
			s = "+" + s
		} else if spec.flagSpace && n >= 0 {
			s = " " + s
		}
		return padNumPyFmt(spec, s, false), nil
	case 'o':
		n, err := pyFmtInt(spec, arg)
		if err != nil {
			return "", err
		}
		s := strconv.FormatInt(n, 8)
		if spec.flagAlt {
			s = "0o" + s
		}
		if spec.flagPlus && n >= 0 {
			s = "+" + s
		}
		return padNumPyFmt(spec, s, false), nil
	case 'x', 'X':
		n, err := pyFmtInt(spec, arg)
		if err != nil {
			return "", err
		}
		s := strconv.FormatInt(n, 16)
		if spec.conv == 'X' {
			s = strings.ToUpper(s)
		}
		if spec.flagAlt && n != 0 {
			prefix := "0x"
			if spec.conv == 'X' {
				prefix = "0X"
			}
			s = prefix + s
		}
		if spec.flagPlus && n >= 0 {
			s = "+" + s
		}
		return padNumPyFmt(spec, s, false), nil
	case 'e', 'E', 'f', 'F', 'g', 'G':
		f, ok := pyNumericValue(arg)
		if !ok {
			return "", fmt.Errorf("$F.mod: %%%c format: a real number is required, not %s", spec.conv, pyTypeName(arg))
		}
		if math.IsInf(f, 0) || math.IsNaN(f) {
			s := "inf"
			if math.IsNaN(f) {
				s = "nan"
			} else if math.Signbit(f) {
				s = "-inf"
			} else if spec.flagPlus {
				s = "+inf"
			}
			if spec.conv == 'E' || spec.conv == 'F' || spec.conv == 'G' {
				s = strings.ToUpper(s)
			}
			return padNumPyFmt(spec, s, false), nil
		}
		prec := spec.precision
		if !spec.hasPrec {
			prec = 6
		}
		verb := fmt.Sprintf("%%%s%s%s.%df", boolSign(spec.flagMinus, "-"), boolSign(spec.flagZero, "0"),
			boolSign(spec.flagPlus, "+"), prec)
		switch spec.conv {
		case 'e', 'E':
			verb = fmt.Sprintf("%%%s%s%s.%de", boolSign(spec.flagMinus, "-"), boolSign(spec.flagZero, "0"),
				boolSign(spec.flagPlus, "+"), prec)
		case 'g', 'G':
			verb = fmt.Sprintf("%%%s%s%s.%dg", boolSign(spec.flagMinus, "-"), boolSign(spec.flagZero, "0"),
				boolSign(spec.flagPlus, "+"), prec)
		}
		s := fmt.Sprintf(verb, f)
		if spec.conv == 'E' || spec.conv == 'F' || spec.conv == 'G' {
			s = strings.ToUpper(s)
		}
		if spec.flagSpace && f >= 0 && !spec.flagPlus {
			s = " " + s
		}
		return padNumPyFmt(spec, s, false), nil
	case 'c':
		switch t := arg.(type) {
		case string:
			if utf8.RuneCountInString(t) != 1 {
				return "", fmt.Errorf("$F.mod: %%c requires an int or a single-character string")
			}
			return padPyFmt(spec, t), nil
		}
		n, ok := pyIntValue(arg)
		if !ok {
			return "", fmt.Errorf("$F.mod: %%c requires an int or a single-character string")
		}
		return padPyFmt(spec, string(rune(n))), nil
	}
	return "", fmt.Errorf("$F.mod: unsupported format character %q", string(spec.conv))
}

// pyFmtInt coerces arg to int64 the way Python's %d/%i/%o/%x/%X do: ints pass
// through, bools are 0/1, floats truncate toward zero; anything else is a
// Python TypeError.
func pyFmtInt(spec pyFmtSpec, arg any) (int64, error) {
	if n, ok := pyIntValue(arg); ok {
		return n, nil
	}
	if f, ok := pyFloatValue(arg); ok {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, fmt.Errorf("$F.mod: cannot convert float %s to integer", pyFloatRepr(f))
		}
		return int64(f), nil // Python int() truncates toward zero
	}
	_ = spec
	return 0, fmt.Errorf("$F.mod: %%d format: a real number is required, not %s", pyTypeName(arg))
}

// padPyFmt applies width / '-' / '0' padding to an already-rendered string
// conversion (Python pads string conversions with spaces only).
func padPyFmt(spec pyFmtSpec, s string) string {
	if spec.hasPrec && spec.conv == 's' && len(s) > spec.precision {
		// Python %.Ns truncates by characters.
		runes := []rune(s)
		if len(runes) > spec.precision {
			s = string(runes[:spec.precision])
		}
	}
	if spec.width <= utf8.RuneCountInString(s) {
		return s
	}
	pad := spec.width - utf8.RuneCountInString(s)
	if spec.flagMinus {
		return s + strings.Repeat(" ", pad)
	}
	return strings.Repeat(" ", pad) + s
}

// padNumPyFmt applies width / '-' / '0' padding to a numeric rendering; zero
// padding keeps the sign/prefix in front (Python/C semantics).
func padNumPyFmt(spec pyFmtSpec, s string, _ bool) string {
	if spec.width <= len(s) {
		return s
	}
	pad := spec.width - len(s)
	if spec.flagMinus {
		return s + strings.Repeat(" ", pad)
	}
	if spec.flagZero {
		if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "+") || strings.HasPrefix(s, " ") {
			return s[:1] + strings.Repeat("0", pad) + s[1:]
		}
		return strings.Repeat("0", pad) + s
	}
	return strings.Repeat(" ", pad) + s
}

func boolSign(cond bool, s string) string {
	if cond {
		return s
	}
	return ""
}

// pyTypeName names a value the way Python's type(...).__name__ does for the
// expression value domain (used in error messages).
func pyTypeName(v any) string {
	switch v.(type) {
	case nil:
		return "NoneType"
	case bool:
		return "bool"
	case string:
		return "str"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "int"
	case float32, float64:
		return "float"
	case json.Number:
		return "number"
	case []any:
		return "list"
	case map[string]any:
		return "dict"
	default:
		return fmt.Sprintf("%T", v)
	}
}
