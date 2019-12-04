package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type (
	readQueueOptions struct {
		svc               *sqs.SQS
		queueURL          string
		visibilityTimeout int64
		msg               chan []message
		err               chan error
		ctx               context.Context
		wg                *sync.WaitGroup
	}
	readQueueResult struct {
		Extracted string    `json:"extracted"`
		Queue     string    `json:"queueUrl"`
		Messages  []message `json:"messages"`
	}
	message struct {
		CustAttrib map[string]string `json:"customAttributes"`
		AwsAttrib  map[string]string `json:"awsAttributes"`
		Message    flexiString       `json:"message"`
	}
)

func readQueueData(opts readQueueOptions) {
	defer opts.wg.Done()
	for {
		select {
		case <-opts.ctx.Done():
			return
		default:
			var visibility int64 = 20
			if opts.visibilityTimeout > 0 {
				visibility = opts.visibilityTimeout
			}
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
					VisibilityTimeout:   aws.Int64(visibility),
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
	for _, m := range input.Messages {
		msg := message{
			AwsAttrib:  make(map[string]string),
			CustAttrib: make(map[string]string),
		}
		for k, v := range m.Attributes {
			val := "<nil>"
			if v != nil {
				if ok, ts := isTimestamp(k, *v); ok {
					msg.AwsAttrib["_"+k] = formatTimestamp(ts)
				}
				val = *v

			}
			msg.AwsAttrib[k] = val
		}
		for k, v := range m.MessageAttributes {
			val := "<nil>"
			if v != nil {
				val = *v.StringValue
			}
			msg.CustAttrib[k] = val
		}
		if m.Body != nil {
			msg.Message = flexiString(*m.Body)
		}
		result = append(result, msg)
	}
	return result
}

func readMessages(options readQueueOptions) {
	for i := 0; i < 10; i++ {
		options.wg.Add(1)
		go readQueueData(options)
	}
	done := signalWaitGroupDone(options.wg)
	results := readQueueResult{
		Extracted: time.Now().UTC().Format(msRFCTimeFormat),
		Queue:     options.queueURL,
	}
	var sum summary
	defer func() {
		results.write()
		sum.write()
	}()

	for {
		select {
		case <-done:
			return
		case err := <-options.err:
			_, _ = fmt.Fprintf(os.Stderr, "Error:%v\n", err)
		case msg := <-options.msg:
			results.add(msg)
			sum.add(msg)
		}
	}
}

func readOptions(ctx context.Context, svc *sqs.SQS, queueUrl string, visibility int64) readQueueOptions {
	return readQueueOptions{
		svc:               svc,
		queueURL:          queueUrl,
		visibilityTimeout: visibility,
		ctx:               ctx,
		msg:               make(chan []message),
		err:               make(chan error),
		wg:                &sync.WaitGroup{},
	}
}

// write json result
func (r *readQueueResult) write() {
	if len(r.Messages) > 0 {
		buf, err := jsonMarshal(r)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "ERROR:%v\n", err)
		}
		err = ioutil.WriteFile("result.json", buf, 0666)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "ERROR2:%v\n", err)
		}
	}
}

func (r *readQueueResult) add(messages []message) {
	r.Messages = append(r.Messages, messages...)
}
