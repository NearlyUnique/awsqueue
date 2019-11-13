package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func listQueues(svc *sqs.SQS, filter string, allMessages bool) (QueueSearchResult, error) {
	list, err := svc.ListQueues(nil)
	if err != nil {
		return QueueSearchResult{}, err
	}
	result := readQueueAttrs(svc, list, filter, allMessages)
	return result, nil
}

func readQueueAttrs(svc *sqs.SQS, list *sqs.ListQueuesOutput, filter string, allMessages bool) QueueSearchResult {
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
					parts := strings.Split(q, "/")
					attrs := map[string]string{
						"QueueUrl": q,
						"Name":     parts[len(parts)-1],
					}
					for key, value := range attr.Attributes {
						attrs[key] = formatValue(key, *value)
					}
					ch <- attrs
				}
			}(*q)
		}
	}
	var done sync.WaitGroup
	done.Add(1)
	var results QueueSearchResult
	go func() {
		results = writeJSON(f, allMessages, ch, &done)
	}()
	wg.Wait()
	close(ch)
	done.Wait()
	return results
}
