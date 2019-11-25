package main

import (
	"encoding/json"
	"io/ioutil"
)

type summary struct {
	msgAttribs map[string]map[string]int
	msgCount   int
	limit      int
}

func (s *summary) add(msg []message) {
	for _, m := range msg {
		s.addOne(m)
	}
}
func (s *summary) addOne(msg message) {
	s.msgCount++
	if len(s.msgAttribs) == 0 {
		s.msgAttribs = make(map[string]map[string]int)
	}
	var ok bool
	var m map[string]int
	for k, v := range msg.MsgAttrib {
		if m, ok = s.msgAttribs[k]; !ok {
			m = make(map[string]int)
			s.msgAttribs[k] = m
		}
		m[v]++
	}
}

func (s *summary) write() {
	if len(s.msgAttribs) > 0 {
		buf, _ := json.Marshal(s.msgAttribs)
		_ = ioutil.WriteFile("summary.json", buf, 0666)
	}
}

func (s *summary) analyse() {
	for k, v := range s.msgAttribs {
		if len(v) == s.msgCount && s.msgCount > 1 {
			var randomK string
			for randomK, _ = range v {
				break
			}
			s.msgAttribs[k] = map[string]int{"$UNIQUE:" + randomK: s.msgCount}
		}
	}
}
