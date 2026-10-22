package ir

import "fmt"

// RejectLoneSurrogateEscapes scans a raw JSON document for \uD800-\uDFFF
// escape sequences that decode into lone surrogate code points and rejects
// them by name. Paired high+low escapes decode to real non-BMP characters
// and stay accepted; so does a literal U+FFFD (an ordinary code point).
//
// Why: Python's json.loads accepts \udXXX escapes into a str holding lone
// surrogates, but compute_flow_definition_hash refuses the definition at
// its .encode("utf-8") step (plaita flow_worker.py:941-945 — A129-01
// poison pill, A129-03 fingerprint collapse). encoding/json instead folds
// them to U+FFFD, so a decoder-only Go would hash — and run — definitions
// the Python pool can never hash: the mixed-fleet tear A1 r132 card G-1
// closes. Both enforcement points route through here: the engine load path
// (load) and the worker fingerprint (worker.canonicalFlowDefinitionJSON).
//
// Call only AFTER the JSON decoder accepted the document: the scan relies
// on valid string/escape structure — malformed input is the decoder's
// rejection, and escapes never occur outside strings in valid JSON.
func RejectLoneSurrogateEscapes(raw []byte) error {
	inStr := false
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '"':
			inStr = !inStr
		case '\\':
			if !inStr || i+1 >= len(raw) {
				continue
			}
			if esc := raw[i+1]; esc != 'u' {
				i++ // skip the escaped char: \\ followed by "ud800" text is not an escape
				continue
			}
			cp, ok := hexQuad(raw[i+2:])
			if !ok {
				continue // malformed \u — the decoder already rejected the document
			}
			switch {
			case cp >= 0xDC00 && cp <= 0xDFFF:
				return loneSurrogateErr(raw, i)
			case cp >= 0xD800 && cp <= 0xDBFF:
				// A high surrogate is legal only when a \uDC00-\uDFFF
				// escape immediately follows and pairs with it.
				if i+7 < len(raw) && raw[i+6] == '\\' && raw[i+7] == 'u' {
					if lo, ok := hexQuad(raw[i+8:]); ok && lo >= 0xDC00 && lo <= 0xDFFF {
						i += 11 // consume the pair; the loop's ++ lands past it
						continue
					}
				}
				return loneSurrogateErr(raw, i)
			default:
				i += 4 // skip the four hex digits of a plain \uXXXX
			}
		}
	}
	return nil
}

func hexQuad(b []byte) (rune, bool) {
	if len(b) < 4 {
		return 0, false
	}
	var cp rune
	for _, c := range b[:4] {
		cp <<= 4
		switch {
		case c >= '0' && c <= '9':
			cp |= rune(c - '0')
		case c >= 'a' && c <= 'f':
			cp |= rune(c-'a') + 10
		case c >= 'A' && c <= 'F':
			cp |= rune(c-'A') + 10
		default:
			return 0, false
		}
	}
	return cp, true
}

func loneSurrogateErr(raw []byte, escStart int) error {
	end := escStart + 6
	if end > len(raw) {
		end = len(raw)
	}
	return fmt.Errorf(
		"lone surrogate escape %s at offset %d: not encodable to UTF-8 — outside the domain Python's flow-definition hash accepts",
		raw[escStart:end], escStart,
	)
}
