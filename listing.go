package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type listQueueOptions struct {
	svc         *sqs.SQS
	filter      string
	allMessages bool
	ctx         context.Context
}

func listQueues(options listQueueOptions) (QueueSearchResult, error) {
	list, err := options.svc.ListQueues(nil)
	if err != nil {
		return QueueSearchResult{}, err
	}
	result := readQueueAttrs(options, list)
	return result, nil
}

func readQueueAttrs(options listQueueOptions, list *sqs.ListQueuesOutput) QueueSearchResult {
	var wg sync.WaitGroup
	ch := make(chan map[string]flexiString)

	results := QueueSearchResult{
		Filter: options.filter,
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

				attr, err := options.svc.GetQueueAttributes(&attrQuery)
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

	results.AllMessages = options.allMessages
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
		buf, err := jsonMarshal(result)
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

func (result QueueSearchResult) matchesFilter(queue string) bool {
	return result.Filter == "" || strings.Contains(strings.ToLower(queue), strings.ToLower(result.Filter))
}

func (result QueueSearchResult) filteredQueues() []map[string]flexiString {
	//if len(result.Attrs) == 1 {
	//	// one match, all good
	//	return result.Attrs[:1]
	//}
	var filtered []map[string]flexiString
	for _, attr := range result.Attrs {
		hasMessages := attr[sqs.QueueAttributeNameApproximateNumberOfMessages] != "0" && attr[sqs.QueueAttributeNameApproximateNumberOfMessages] != ""
		if (result.AllMessages || (!result.AllMessages && hasMessages)) &&
			result.matchesFilter(attr[AttrKeyQueueName].String()) {
			filtered = append(filtered, attr)
		}
	}
	return filtered
}
func (result QueueSearchResult) exactMatch() map[string]flexiString {
	// exact match across any Queue?
	for _, attr := range result.Attrs {
		if strings.EqualFold(result.Filter, attr[AttrKeyQueueName].String()) {
			// several matches but an exact (case insensitive) match, all good
			return attr
		}
	}
	return nil
}
