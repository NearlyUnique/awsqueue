package main

import (
	"errors"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/pflag"
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

func canResolveSingleQueue(result QueueSearchResult) (bool, string) {
	if len(result.Attrs) == 1 {
		// one match, all good
		return true, result.Attrs[0][AttrKeyQueueUrl]
	}
	for _, attr := range result.Attrs {
		if strings.EqualFold(result.Filter, attr["_Name"]) {
			// several matches but an exact (case insensitive) match, all good
			return true, attr[AttrKeyQueueUrl]
		}
	}
	// ambiguous, so be more specific
	return false, ""
}

type CmdAction string

const (
	CmdActionList  CmdAction = "list"
	CmdActionRead  CmdAction = "read"
	CmdActionWrite CmdAction = "write"
)

func cmdAction(fs *pflag.FlagSet) (CmdAction, error) {
	read := fs.Lookup("read").Value.String() == "true"
	write := fs.Lookup("write-source").Value.String() != ""
	if read && write {
		return "", errors.New("cannot specify both read and write")
	}
	if read {
		return CmdActionRead, nil
	}
	if write {
		return CmdActionWrite, nil
	}
	return CmdActionList, nil
}
