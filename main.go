package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

func main() {
	filter := pflag. ("filter", "", "substring search to filter queues")
	asJson := pflag.Bool("asJson", false, "Output format defaults to summary (count,name), asJson fives fuller output")
	allMessages := pflag.Bool("all", false, "If true shows message attributes event when there are no messages in the queue")
	regionArg := pflag.String("region", os.Getenv("AWS_REGION"), "AWS region, defaults from env variable (AWS_REGION) then to eu-west-1")
	dump := pflag.Bool("dump", false, "dump messages and meta data, will only run if a single queue can be resolved via --filter")
	pflag.Parse()

	if *regionArg == "" {
		*regionArg = "eu-west-1"
	}
	sess, err := session.NewSession(&aws.Config{Region: aws.String(*regionArg)})
	if err != nil {
		log.Fatal(err)
	}
	svc := sqs.New(sess)
	result, err := listQueues(svc, *filter, *allMessages)
	if err != nil {
		log.Fatal(err)
	}

	if *dump {
		if len(result.Attrs) == 1 {
			dumpMessages(readOptions(svc, result.Attrs[0]["QueueUrl"]))
		} else {
			_, _ = fmt.Fprintln(os.Stderr, "Did not find exactly one queue, fix filter first")
		}
		return
	}
	if *asJson {
		buf, err := json.Marshal(result)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed marshalling json: %v\n", err)
			return
		}
		fmt.Println(string(buf))
		return
	}

	for _, attr := range result.Attrs {
		fmt.Printf("%5s %s\n", attr["ApproximateNumberOfMessages"], attr["Name"])
	}
}

func readOptions(svc *sqs.SQS, queueUrl string) queueReadOptions {
	return queueReadOptions{
		svc:      svc,
		queueURL: queueUrl,
		ctx:      context.Background(),
		msg:      make(chan []message),
		err:      make(chan error),
		wg:       &sync.WaitGroup{},
	}
}

func signalWaitGroupDone(wg *sync.WaitGroup) chan struct{} {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		ch <- struct{}{}
	}()
	return ch
}
