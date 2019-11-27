package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/spf13/pflag"
)

func registerForCtrlC() chan os.Signal {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	return ch
}

func canResolveSingleQueue(result QueueSearchResult) (bool, string) {
	if len(result.Attrs) == 1 {
		// one match, all good
		return true, result.Attrs[0][AttrKeyQueueUrl].String()
	}
	// single match allowing for filters and all-Messages
	queueUrl := ""
	for _, attr := range result.Attrs {
		hasMessages := attr[sqs.QueueAttributeNameApproximateNumberOfMessages] != "0" && attr[sqs.QueueAttributeNameApproximateNumberOfMessages] != ""
		if (result.AllMessages || (!result.AllMessages && hasMessages)) &&
			result.matchesFilter(attr[AttrKeyQueueName].String()) {
			if queueUrl != "" {
				// more than one
				queueUrl = ""
				break
			}
			queueUrl = attr[AttrKeyQueueUrl].String()
		}
	}
	if queueUrl != "" {
		return true, queueUrl
	}
	// exact match across any Queue?
	for _, attr := range result.Attrs {
		if strings.EqualFold(result.Filter, attr[AttrKeyQueueName].String()) {
			// several matches but an exact (case insensitive) match, all good
			return true, attr[AttrKeyQueueUrl].String()
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

type flexiString string

func (fs flexiString) String() string {
	return string(fs)
}

// MarshalJSON custom
func (fs flexiString) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("")
	//buffer.WriteString("}")
	if len(fs) >= 2 && fs[0] == '{' && fs[len(fs)-1] == '}' {
		return []byte(fs), nil
	}
	s := string(fs)
	buf, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	_, err = buffer.Write(buf)
	return buffer.Bytes(), err
}
