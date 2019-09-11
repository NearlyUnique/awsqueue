package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	filter := flag.String("filter", "", "substring search to filter queues")
	allMessages := flag.Bool("all-messages", false, "If trye shows message attributes event when there are no messages in the queue")
	regionArg := flag.String("region", os.Getenv("AWS_REGION"), "AWS region, defaults from env variable (AWS_REGION) then to eu-west-1")
	flag.Parse()

	if *regionArg == "" {
		*regionArg = "eu-west-1"
	}
	sess, err := session.NewSession(&aws.Config{Region: aws.String(*regionArg)})
	if err != nil {
		log.Fatal(err)
	}
	svc := sqs.New(sess)
	list, err := svc.ListQueues(nil)
	if err != nil {
		log.Fatal(err)
	}
	readQueueAttrs(*filter, list, svc, *allMessages)
}

func readQueueAttrs(filter string, list *sqs.ListQueuesOutput, svc *sqs.SQS, allMessages bool) {
	var wg sync.WaitGroup
	ch := make(chan map[string]string, len(list.QueueUrls))
	// toLower is not cool but the list is short, as are the strings
	f := strings.ToLower(filter)
	for _, q := range list.QueueUrls {
		if filter == "" || strings.Contains(strings.ToLower(*q), f) {
			wg.Add(1)
			go func(q string) {
				defer wg.Done()
				attrQuery := sqs.GetQueueAttributesInput{
					QueueUrl:       &q,
					AttributeNames: []*string{aws.String("All")},
				}

				attr, err := svc.GetQueueAttributes(&attrQuery)
				if err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "Failed at: %v\n", q)
					log.Fatal(err)
				}
				msgCount := *attr.Attributes["ApproximateNumberOfMessages"]
				if allMessages || msgCount != "0" {
					attrs := map[string]string{"QueueUrl": q}
					for key, value := range attr.Attributes {
						attrs[key] = *value
					}
					ch <- attrs
				}
			}(*q)
		}
	}
	var done sync.WaitGroup
	done.Add(1)
	go writeJSON(f, allMessages, ch, &done)
	wg.Wait()
	close(ch)
	done.Wait()
}

func writeJSON(filter string, onlyMessages bool, ch chan map[string]string, done *sync.WaitGroup) {
	defer done.Done()
	results := struct {
		Filter      string
		AllMessages bool
		Attrs       []map[string]string
	}{
		Filter:      filter,
		AllMessages: onlyMessages,
	}
	for a := range ch {
		results.Attrs = append(results.Attrs, a)
	}
	buf, err := json.Marshal(results)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed marshalling json: %v\n", err)
		return
	}
	fmt.Println(string(buf))
}
