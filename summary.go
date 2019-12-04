package main

import (
	"fmt"
	"io/ioutil"
	"os"
)

type (
	timeRange struct {
		From    int64  `json:"from"`
		FromStr string `json:"fromDtm"`
		To      int64  `json:"to"`
		ToStr   string `json:"toDtm"`
	}
	summary struct {
		MsgCount   int                       `json:"messageCount"`
		MsgAttribs map[string]map[string]int `json:"customAttributes"`
		Timestamps map[string]timeRange      `json:"timestamps"`
		limit      int
	}
)

const msRFCTimeFormat = "2006-01-02 15:04:05.999"

func (t timeRange) record(value int64) timeRange {
	if t.From == 0 && t.To == 0 {
		t.From = value
		t.To = value
	} else if value < t.From {
		t.From = value
	} else if value > t.To {
		t.To = value
	}
	return t
}

func (t timeRange) format() timeRange {
	return timeRange{
		From:    t.From,
		To:      t.To,
		FromStr: fromUnixMilli(t.From).Format(msRFCTimeFormat),
		ToStr:   fromUnixMilli(t.To).Format(msRFCTimeFormat),
	}
}

func (s *summary) add(msg []message) {
	for _, m := range msg {
		s.addOne(m)
	}
}

func (s *summary) addOne(msg message) {
	s.MsgCount++
	if len(s.MsgAttribs) == 0 {
		s.MsgAttribs = make(map[string]map[string]int)
		s.Timestamps = make(map[string]timeRange)
	}
	var ok bool
	var m map[string]int
	for k, v := range msg.CustAttrib {
		if m, ok = s.MsgAttribs[k]; !ok {
			m = make(map[string]int)
			s.MsgAttribs[k] = m
		}
		m[v]++
	}
	for k, v := range msg.AwsAttrib {
		if ok, ts := isTimestamp(k, v); ok {
			s.Timestamps[k] = s.Timestamps[k].record(ts)
		}
	}
}

func (s *summary) write() {
	s.analyse()
	buf, _ := jsonMarshal(s)
	err := ioutil.WriteFile("summary.json", buf, 0666)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "ERROR:failed summary.json %v", err)
	}
}

func (s *summary) analyse() {
	for k, v := range s.MsgAttribs {
		if len(v) == s.MsgCount && s.MsgCount > 1 {
			var randomK string
			for randomK, _ = range v {
				break
			}
			s.MsgAttribs[k] = map[string]int{"$UNIQUE:" + randomK: s.MsgCount}
		}
	}
	for k := range s.Timestamps {
		s.Timestamps[k] = s.Timestamps[k].format()
	}
}
