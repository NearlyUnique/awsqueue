package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Result struct {
	Filter      string              `json:"filter"`
	AllMessages bool                `json:"allMessages"`
	Attrs       []map[string]string `json:"attrs"`
}

func main() {
	filter := flag.String("filter", "", "substring search to filter queues")
	format := flag.String("format", "json", "Output format, json or summary (count,name)")
	allMessages := flag.Bool("all-messages", false, "If true shows message attributes event when there are no messages in the queue")
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
	result := readQueueAttrs(*filter, list, svc, *allMessages)

	switch *format {
	case "summary":
		for _, attr := range result.Attrs {
			fmt.Printf("%5s %s\n", attr["ApproximateNumberOfMessages"], attr["name"])
		}
	default:
		buf, err := json.Marshal(result)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed marshalling json: %v\n", err)
			return
		}
		fmt.Println(string(buf))
	}

}

func readQueueAttrs(filter string, list *sqs.ListQueuesOutput, svc *sqs.SQS, allMessages bool) Result {
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
						"queueUrl": q,
						"name":     parts[len(parts)-1],
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
	var results Result
	go func() {
		results = writeJSON(f, allMessages, ch, &done)
	}()
	wg.Wait()
	close(ch)
	done.Wait()
	return results
}

func writeJSON(filter string, onlyMessages bool, ch chan map[string]string, done *sync.WaitGroup) Result {
	defer done.Done()
	results := Result{
		Filter:      filter,
		AllMessages: onlyMessages,
	}
	for a := range ch {
		results.Attrs = append(results.Attrs, a)
	}
	return results
}

func formatValue(key, value string) string {
	if !strings.HasSuffix(key, "Timestamp") {
		return value
	}
	ts, err := strconv.Atoi(value)
	if err != nil {
		return value
	}
	t := time.Unix(int64(ts), 0)
	return t.String()
}
