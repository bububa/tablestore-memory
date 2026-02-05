package model

import "time"

// --------------------
// utils
// --------------------

func CurrentTimeMicroseconds() int64 {
	return time.Now().UnixMicro()
}
