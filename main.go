package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fatih/color"
)

var (
	yellow = color.New(color.FgYellow).SprintFunc()
	cyan   = color.New(color.FgCyan).SprintFunc()
)

func main() {
	filter := flag.String("filter", "", "substring search to filter queues")
	onlyMessages := flag.Bool("only-messages", false, "show only queues with messages")
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
	// toLower is not cool but the list is short, as are the strings
	f := strings.ToLower(*filter)
	for i, q := range list.QueueUrls {
		if *filter == "" || strings.Contains(strings.ToLower(*q), f) {
			attrQuery := sqs.GetQueueAttributesInput{
				QueueUrl:       q,
				AttributeNames: []*string{aws.String("All")},
			}
			attr, err := svc.GetQueueAttributes(&attrQuery)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed at %d: %v\n", i, *q)
				log.Fatal(err)
			}
			msgCount := *attr.Attributes["ApproximateNumberOfMessages"]
			if !*onlyMessages || msgCount != "0" {
				fmt.Printf("%s: %s\n", cyan("QueueUrl"), yellow(*q))
				for k, v := range attr.Attributes {
					fmt.Printf("\t%s: %s\n", cyan(k), yellow(display(k, *v)))
				}
			}
		}
	}
}

func display(key, value string) string {
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
