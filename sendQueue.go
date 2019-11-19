package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type (
	sendOptions struct {
		svc      *sqs.SQS
		queueURL string
		err      chan error
		ctx      context.Context
	}
)

func sendMessages(options sendOptions) {
	finite := []string{"finite_1", "finite_2", "finite_3"}
	//idPrefix := strconv.Itoa(int(time.Now().Unix()))
	for i := 0; i < 100; i++ {
		body := fmt.Sprintf("some content %02d", i)
		id := fmt.Sprintf("unique_%02d", i)
		attribs := map[string]*sqs.MessageAttributeValue{
			"literal": msgAttrVal("literal_value"),
			"finite":  msgAttrVal(finite[i%len(finite)]),
			"unique":  msgAttrVal(id),
		}
		target := sqs.SendMessageInput{
			MessageAttributes: attribs,
			MessageBody:       &body,
			// this is only valid for FIFO queues
			//MessageDeduplicationId: aws.String(idPrefix + id),
			QueueUrl: &options.queueURL,
		}

		//_ = target
		_, err := options.svc.SendMessageWithContext(options.ctx, &target)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error Sending: %v\n", err)
			//options.err <- err
		}
	}
}
func msgAttrVal(value string) *sqs.MessageAttributeValue {
	return &sqs.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(value),
	}
}
