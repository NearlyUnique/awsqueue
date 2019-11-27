package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_a_single_timestamp_sets_to_and_from(t *testing.T) {
	const key = "ApproximateFirstReceiveTimestamp"
	var dtm int64 = 1574154612615
	dtmStr := "1574154612615"
	fmtDtm := "2019-11-19T09:10:12.615"

	sum := summary{}
	sum.addOne(message{Attrib: map[string]string{
		key: dtmStr,
	}})

	assert.Equal(t, dtm, sum.Timestamps[key].From)
	assert.Equal(t, dtm, sum.Timestamps[key].To)

	sum.analyse()

	assert.Equal(t, fmtDtm, sum.Timestamps[key].FromStr)
	assert.Equal(t, fmtDtm, sum.Timestamps[key].ToStr)
}

func Test_recording_a_timestamp(t *testing.T) {
	t.Run("when value is empty a new timeRange is returned with both To and From are set", func(t *testing.T) {
		ts := timeRange{}
		const aValue int64 = 1
		ts = ts.record(aValue)

		assert.Equal(t, aValue, ts.From)
		assert.Equal(t, aValue, ts.To)
	})

	t.Run("when value before both non-zero values, only 'From' is set in response", func(t *testing.T) {
		now := time.Now()
		ts := timeRange{}.record(now.Unix())
		beforeAll := now.Add(-10 * time.Second)
		ts = ts.record(beforeAll.Unix())

		assert.Equal(t, beforeAll.Unix(), ts.From)
		assert.Equal(t, now.Unix(), ts.To)
	})

	t.Run("when value after both non-zero values, only 'To' is set in response", func(t *testing.T) {
		now := time.Now()
		ts := timeRange{}.record(now.Unix())
		afterAll := now.Add(10 * time.Second)
		ts = ts.record(afterAll.Unix())

		assert.Equal(t, now.Unix(), ts.From)
		assert.Equal(t, afterAll.Unix(), ts.To)
	})

	t.Run("when value between both non-zero values there is no change in response", func(t *testing.T) {
		now := time.Now()
		ts := timeRange{}.record(now.Unix())
		nowPlus10Seconds := now.Add(10 * time.Second)
		ts = ts.record(nowPlus10Seconds.Unix())

		nowPlus5Seconds := now.Add(5 * time.Second)
		ts = ts.record(nowPlus5Seconds.Unix())

		assert.Equal(t, now.Unix(), ts.From)
		assert.Equal(t, nowPlus10Seconds.Unix(), ts.To)
	})
}
