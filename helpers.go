package main

import (
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func formatValue(key, value string) string {
	if !strings.HasSuffix(key, "Timestamp") {
		return value
	}
	ts, err := strconv.Atoi(value)
	if err != nil {
		return value
	}
	t := time.Unix(int64(ts), 0)
	return t.String()
}

func registerForCtrlC() chan os.Signal {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	return ch
}

func writeJSON(filter string, onlyMessages bool, ch chan map[string]string, done *sync.WaitGroup) QueueSearchResult {
	defer done.Done()
	results := QueueSearchResult{
		Filter:      filter,
		AllMessages: onlyMessages,
	}
	for a := range ch {
		results.Attrs = append(results.Attrs, a)
	}
	return results
}
