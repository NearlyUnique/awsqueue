package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type (
	queueReadOptions struct {
		svc      *sqs.SQS
		queueURL string
		msg      chan []message
		err      chan error
		ctx      context.Context
		wg       *sync.WaitGroup
	}
	message struct {
		MsgAttrib map[string]string `json:"msgAttribs"`
		Attrib    map[string]string `json:"meta"`
		Message   string            `json:"message"`
	}
)

func readQueueData(opts queueReadOptions) {
	defer opts.wg.Done()
	for {
		select {
		case <-opts.ctx.Done():
			return
		default:
			result, err := opts.svc.ReceiveMessageWithContext(opts.ctx,
				&sqs.ReceiveMessageInput{
					AttributeNames: []*string{
						aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
						aws.String(sqs.MessageSystemAttributeNameApproximateReceiveCount),
						aws.String(sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp),
					},
					MessageAttributeNames: []*string{
						aws.String(sqs.QueueAttributeNameAll),
					},
					QueueUrl:            &opts.queueURL,
					MaxNumberOfMessages: aws.Int64(10),
					VisibilityTimeout:   aws.Int64(20), // 20 seconds
					WaitTimeSeconds:     aws.Int64(0),
				})

			if err == nil {
				if len(result.Messages) == 0 {
					return
				}
				opts.msg <- simplifyMessage(result)
			} else {
				opts.err <- err
			}
		}
	}
}

func simplifyMessage(input *sqs.ReceiveMessageOutput) []message {
	var result []message
	msg := message{
		Attrib:    make(map[string]string),
		MsgAttrib: make(map[string]string),
	}
	for _, m := range input.Messages {
		for k, v := range m.Attributes {
			val := "<nil>"
			if v != nil {
				val = *v
			}
			msg.Attrib[k] = val
		}
		for k, v := range m.MessageAttributes {
			val := "<nil>"
			if v != nil {
				val = *v.StringValue
			}
			msg.MsgAttrib[k] = val
		}
		if m.Body != nil {
			msg.Message = *m.Body
		}
		result = append(result, msg)
	}
	return result
}

func dumpMessages(options queueReadOptions) {
	var cancel func()
	options.ctx, cancel = context.WithCancel(options.ctx)
	sigEnd := registerForCtrlC()
	for i := 0; i < 10; i++ {
		options.wg.Add(1)
		go readQueueData(options)
	}
	done := signalWaitGroupDone(options.wg)
	var results []message
	var sum summary
	defer func() {
		if len(results) > 0 {
			buf, _ := json.Marshal(results)
			_ = ioutil.WriteFile("result.json", buf, 0666)
		}
		sum.write()
	}()

	for {
		select {
		case <-sigEnd:
			fmt.Println("User canceled, stopping...")
			cancel()
		case <-done:
			fmt.Println("Finished")
			return
		case err := <-options.err:
			_, _ = fmt.Fprintf(os.Stderr, "Error:%v\n", err)
		case msg := <-options.msg:
			results = append(results, msg...)
			sum.add(msg)
		}
	}
}
