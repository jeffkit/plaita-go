package plaitaexpr

// date_functions.go ports plaita's $F.now / $F.today — Python
// datetime.now().strftime(fmt) / date.today().strftime(fmt)
// (expression.py _fn_now/_fn_today). The optional format is a STRFTIME
// layout (the Go reference layout made "%Y" render literally, silently
// wrong values); defaults match Python's.

import "time"

func now(format ...string) string {
	layout := "%Y-%m-%d %H:%M:%S"
	if len(format) > 0 {
		layout = format[0]
	}
	return pyStrftime(time.Now(), layout)
}

func today(format ...string) string {
	layout := "%Y-%m-%d"
	if len(format) > 0 {
		layout = format[0]
	}
	nowTime := time.Now()
	todayTime := time.Date(nowTime.Year(), nowTime.Month(), nowTime.Day(), 0, 0, 0, 0, time.Local)
	return pyStrftime(todayTime, layout)
}
