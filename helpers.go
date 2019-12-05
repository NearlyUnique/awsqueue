package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func registerForCtrlC(cancel func()) {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-ch
		_, _ = fmt.Fprintln(os.Stderr, "User canceled, stopping...")
		cancel()
	}()
}

func signalWaitGroupDone(wg *sync.WaitGroup) chan struct{} {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		ch <- struct{}{}
	}()
	return ch
}

type CmdAction string

const (
	CmdActionList  CmdAction = "list"
	CmdActionRead  CmdAction = "read"
	CmdActionWrite CmdAction = "write"
)

func cmdAction(fs cliFlags) (CmdAction, error) {
	read := fs.read
	write := fs.sendMsgSrc != ""
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
	buf, err := jsonMarshal(s)
	if err != nil {
		return nil, err
	}
	_, err = buffer.Write(buf)
	return buffer.Bytes(), err
}

func jsonMarshal(v interface{}) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}
