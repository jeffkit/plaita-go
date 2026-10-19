package plaitaexpr

import "time"

func now(format ...string) string {
	fmt := time.DateTime
	if len(format) > 0 {
		fmt = format[0]
	}
	return time.Now().Format(fmt)
}

func today(format ...string) string {
	fmt := time.DateOnly
	if len(format) > 0 {
		fmt = format[0]
	}
	nowTime := time.Now()
	todayTime := time.Date(nowTime.Year(), nowTime.Month(), nowTime.Day(), 0, 0, 0, 0, time.Local)
	return todayTime.Format(fmt)
}
