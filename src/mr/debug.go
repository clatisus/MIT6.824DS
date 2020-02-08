package mr

import "fmt"

const debugEnabled = true

func debug(format string, args ...interface{}) {
	if debugEnabled {
		fmt.Printf(format, args...)
	}
	return
}
