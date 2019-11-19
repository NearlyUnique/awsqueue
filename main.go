package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/spf13/pflag"
)

type QueueSearchResult struct {
	Filter      string              `json:"filter"`
	AllMessages bool                `json:"allMessages"`
	Attrs       []map[string]string `json:"attrs"`
}

const (
	AttrKeyQueueUrl  = "_Url"
	AttrKeyQueueName = "_Name"
)

func main() {
	if err := _main(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func _main() error {
	fs := pflag.NewFlagSet("default", pflag.ExitOnError)
	filter := fs.StringP("filter", "f", "", "substring search to filter queues")
	asJson := fs.BoolP("json", "j", false, "Output format defaults to summary (count,name), asJson fives fuller output")
	allMessages := fs.Bool("all", false, "If true shows message attributes event when there are no messages in the queue")
	regionArg := fs.String("region", os.Getenv("AWS_REGION"), "AWS region, defaults from env variable (AWS_REGION) then to eu-west-1")
	_ = fs.Bool("read", false, "read messages and meta data, will only run if a single queue can be resolved via --filter")
	sendMsgSrc := fs.String("write-source", "", "json source file to send messages, will only run if a single queue can be resolved via --filter")
	err := fs.Parse(os.Args[1:])
	if err != nil {
		return err
	}

	if *regionArg == "" {
		*regionArg = "eu-west-1"
	}
	sess, err := session.NewSession(&aws.Config{Region: aws.String(*regionArg)})
	if err != nil {
		return err
	}
	action, err := cmdAction(fs)
	if err != nil {
		return err
	}
	svc := sqs.New(sess)
	result, err := listQueues(svc, *filter, *allMessages)
	if err != nil {
		return err
	}
	switch action {
	case CmdActionList:
		return printList(*asJson, result)
	case CmdActionRead:
		ok, queueUrl := canResolveSingleQueue(result)
		if !ok {
			return errors.New("did not find exactly one queue, fix filter first")
		}
		readMessages(readOptions(context.Background(), svc, queueUrl))
	case CmdActionWrite:
		ok, queueUrl := canResolveSingleQueue(result)
		if !ok {
			return errors.New("did not find exactly one queue, fix filter first")
		}
		_ = sendMsgSrc
		sendMessages(sendOptions{
			queueURL: queueUrl,
			ctx:      context.Background(),
			svc:      svc,
			err:      make(chan error, 10),
		})
	}
	return nil
}

func signalWaitGroupDone(wg *sync.WaitGroup) chan struct{} {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		ch <- struct{}{}
	}()
	return ch
}
