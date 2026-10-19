package plaitaexpr

// strftime.go formats times with Python's strftime directive subset
// (semantic authority: datetime.strftime defaults used by $F.now/$F.today,
// expression.py _fn_now/_fn_today). Unknown directives pass through
// literally, matching the platform strftime behaviour Python delegates to.

import (
	"fmt"
	"strings"
	"time"
)

// pyStrftime renders t with a strftime layout ("%Y-%m-%d %H:%M:%S").
func pyStrftime(t time.Time, layout string) string {
	var b strings.Builder
	for i := 0; i < len(layout); i++ {
		if layout[i] != '%' || i+1 >= len(layout) {
			b.WriteByte(layout[i])
			continue
		}
		i++
		spec, pad := layout[i], true
		if spec == '-' && i+1 < len(layout) { // glibc "no padding" modifier
			pad = false
			i++
			spec = layout[i]
		}
		switch spec {
		case 'Y':
			b.WriteString(fmt.Sprintf("%04d", t.Year()))
		case 'y':
			b.WriteString(fmt.Sprintf("%02d", t.Year()%100))
		case 'C':
			b.WriteString(fmt.Sprintf("%02d", t.Year()/100))
		case 'm':
			b.WriteString(padNum2(int(t.Month()), pad))
		case 'd':
			b.WriteString(padNum2(t.Day(), pad))
		case 'e':
			b.WriteString(fmt.Sprintf("%2d", t.Day()))
		case 'H':
			b.WriteString(padNum2(t.Hour(), pad))
		case 'I':
			b.WriteString(padNum2(hour12(t), true))
		case 'M':
			b.WriteString(padNum2(t.Minute(), pad))
		case 'S':
			b.WriteString(padNum2(t.Second(), pad))
		case 'f':
			b.WriteString(fmt.Sprintf("%06d", t.Nanosecond()/1000))
		case 'j':
			b.WriteString(fmt.Sprintf("%03d", t.YearDay()))
		case 'p':
			if t.Hour() < 12 {
				b.WriteString("AM")
			} else {
				b.WriteString("PM")
			}
		case 'a':
			b.WriteString(t.Weekday().String()[:3])
		case 'A':
			b.WriteString(t.Weekday().String())
		case 'b', 'h':
			b.WriteString(t.Month().String()[:3])
		case 'B':
			b.WriteString(t.Month().String())
		case 'Z':
			name, _ := t.Zone()
			b.WriteString(name)
		case 'z':
			zone, offset := t.Zone()
			_ = zone
			sign := "+"
			if offset < 0 {
				sign = "-"
				offset = -offset
			}
			b.WriteString(fmt.Sprintf("%s%02d%02d", sign, offset/3600, (offset%3600)/60))
		case '%':
			b.WriteByte('%')
		default:
			// Unknown directive: pass through verbatim (platform strftime
			// behaviour that Python inherits).
			b.WriteByte('%')
			if !pad {
				b.WriteByte('-')
			}
			b.WriteByte(spec)
		}
	}
	return b.String()
}

func padNum2(n int, pad bool) string {
	if pad {
		return fmt.Sprintf("%02d", n)
	}
	return fmt.Sprintf("%d", n)
}

func hour12(t time.Time) int {
	h := t.Hour() % 12
	if h == 0 {
		h = 12
	}
	return h
}
