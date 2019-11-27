package main

import (
	"strconv"
	"strings"
	"time"
)

func formatTimestamp(ts int64) string {
	dtm := time.Unix(ts, 0)
	if dtm.Year() > 9999 {
		// a ms timestamp
		dtm = fromUnixMilli(ts)
	}

	return dtm.Format(msRFCTimeFormat)
}

func isTimestamp(key, value string) (bool, int64) {
	if !strings.HasSuffix(key, "Timestamp") {
		return false, 0
	}
	ts, err := strconv.ParseInt(value, 10, 0)
	if err != nil {
		return false, 0
	}
	return true, ts
}

// From https://github.com/Tigraine/go-timemilli/blob/master/timemilli.go
const millisInSecond = 1000
const nsInSecond = 1000000

// Converts Unix Epoch From milliseconds To time.Time
func fromUnixMilli(ms int64) time.Time {
	return time.Unix(ms/int64(millisInSecond), (ms%int64(millisInSecond))*int64(nsInSecond))
}
