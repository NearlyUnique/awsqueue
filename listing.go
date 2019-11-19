package main

import (
	"encoding/json"
	"errors"
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
	ch := make(chan map[string]string)
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
				parts := strings.Split(q, "/")
				attrs := map[string]string{
					AttrKeyQueueUrl:  q,
					AttrKeyQueueName: parts[len(parts)-1],
				}
				for key, value := range attr.Attributes {
					attrs[key] = formatValue(key, *value)
				}
				ch <- attrs
			}(*q)
		}
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	results := QueueSearchResult{
		Filter:      filter,
		AllMessages: allMessages,
	}
	for a := range ch {
		results.Attrs = append(results.Attrs, a)
	}

	return results
}

func printList(asJson bool, result QueueSearchResult) error {
	if !result.AllMessages {
		for i := 0; i < len(result.Attrs); i++ {
			if result.Attrs[i]["ApproximateNumberOfMessages"] == "0" {
				result.Attrs = append(result.Attrs[:i], result.Attrs[i+1:]...)
				i--
			}
		}
	}
	if asJson {
		buf, err := json.Marshal(result)
		if err != nil {
			return errors.New(fmt.Sprintf("failed marshalling json: %v\n", err))
		}
		fmt.Println(string(buf))
		return nil
	}
	for _, attr := range result.Attrs {
		fmt.Printf("%5s %s\n", attr["ApproximateNumberOfMessages"], attr[AttrKeyQueueName])
	}
	return nil
}
