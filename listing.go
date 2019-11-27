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
	ch := make(chan map[string]flexiString)
	// toLower is not cool but the list is short, as are the strings
	results := QueueSearchResult{
		Filter: filter,
	}

	for _, q := range list.QueueUrls {
		if results.matchesFilter(*q) {
			wg.Add(1)
			go func(q string) {
				defer wg.Done()
				attrQuery := sqs.GetQueueAttributesInput{
					QueueUrl:       &q,
					AttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
				}

				attr, err := svc.GetQueueAttributes(&attrQuery)
				if err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "Failed at: %v\n", q)
					log.Fatal(err)
				}
				parts := strings.Split(q, "/")
				attrs := map[string]flexiString{
					AttrKeyQueueUrl:  flexiString(q),
					AttrKeyQueueName: flexiString(parts[len(parts)-1]),
				}
				for key, value := range attr.Attributes {
					if ok, ts := isTimestamp(key, *value); ok {
						attrs["_"+key] = flexiString(formatTimestamp(ts))
					}
					attrs[key] = flexiString(*value)
				}
				ch <- attrs
			}(*q)
		}
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	results.AllMessages = allMessages
	for a := range ch {
		results.Attrs = append(results.Attrs, a)
	}

	return results
}

func printList(asJson bool, result QueueSearchResult) error {
	if !result.AllMessages {
		for i := 0; i < len(result.Attrs); i++ {
			if result.Attrs[i][sqs.QueueAttributeNameApproximateNumberOfMessages] == "0" {
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
		fmt.Printf("%5s %s\n", attr[sqs.QueueAttributeNameApproximateNumberOfMessages], attr[AttrKeyQueueName])
	}
	return nil
}

func (results QueueSearchResult) matchesFilter(queue string) bool {
	return results.Filter == "" || strings.Contains(strings.ToLower(queue), strings.ToLower(results.Filter))
}
