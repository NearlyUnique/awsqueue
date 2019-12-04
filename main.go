package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/spf13/pflag"
	"github.com/vito/go-interact/interact"
)

var version = "0.0.0"
var commit = ""
var date = ""

type QueueSearchResult struct {
	Filter      string                   `json:"filter"`
	AllMessages bool                     `json:"allMessages"`
	Attrs       []map[string]flexiString `json:"awsAttributes"`
}

const (
	AttrKeyQueueUrl  = "_Url"
	AttrKeyQueueName = "_Name"
)

func main() {
	if err := _main(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func _main() error {
	fs := pflag.NewFlagSet("default", pflag.ExitOnError)
	filter := fs.StringP("filter", "f", "", "substring search To filter queues")
	asJson := fs.BoolP("json", "j", false, "Output format defaults To summary (count,name), asJson fives fuller output")
	allMessages := fs.Bool("all", false, "If true shows message attributes event when there are no Messages in the Queue")
	regionArg := fs.String("region", os.Getenv("AWS_REGION"), "AWS region, defaults From env variable (AWS_REGION) then To eu-west-1")
	_ = fs.Bool("read", false, "read Messages and meta data, will only run if a single Queue can be resolved via --filter")
	visibility := fs.Int64P("visibility-timeout", "t", 20, "when --read is specified, messages will be unavailable for this many seconds")
	sendMsgSrc := fs.String("write-source", "", "json source file To send Messages, will only run if a single Queue can be resolved via --filter")
	_ = sendMsgSrc // WIP
	showVersion := fs.Bool("version", false, "display version and exit")
	noInteraction := fs.Bool("no-interaction", false, "for CI and scripts to prevent user interaction")

	err := fs.Parse(os.Args[1:])
	if err != nil {
		return err
	}
	if *showVersion {
		fmt.Printf("%s %s %s\n", version, commit, date)
		return nil
	}

	if *regionArg == "" {
		*regionArg = "eu-west-1"
	}
	sess, err := session.NewSession(&aws.Config{Region: aws.String(*regionArg)})
	if err != nil {
		return err
	}
	action, err := cmdAction(fs)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	registerForCtrlC(cancel)

	svc := sqs.New(sess)
	result, err := listQueues(listQueueOptions{
		svc:         svc,
		filter:      *filter,
		allMessages: *allMessages,
		ctx:         ctx,
	})
	if err != nil {
		return err
	}
	if action == CmdActionList {
		return printList(*asJson, result)
	}

	queueURL, err := resolveQueueUrl(result, interactionType(*noInteraction))
	if err != nil {
		return err
	}

	switch action {
	case CmdActionRead:
		readMessages(readOptions(ctx, svc, queueURL, *visibility))
	case CmdActionWrite:
		sendMessages(sendOptions{
			queueURL: queueURL,
			ctx:      ctx,
			svc:      svc,
			err:      make(chan error, 10),
		})
	}
	return nil
}

type (
	interactionType bool
)

const (
	noInteraction    interactionType = true
	allowInteraction interactionType = false
)

func resolveQueueUrl(result QueueSearchResult, interaction interactionType) (string, error) {
	filtered := result.filteredQueues()
	var queueURL string
	switch l := len(filtered); {
	case l == 1:
		queueURL = filtered[0][AttrKeyQueueUrl].String()
	case l > 1:
		var list []interact.Choice
		for _, attr := range filtered {
			// exact match
			if strings.EqualFold(result.Filter, attr[AttrKeyQueueName].String()) {
				return attr[AttrKeyQueueUrl].String(), nil
			}
			list = append(list,
				interact.Choice{
					Display: fmt.Sprintf("%5s %s", attr[sqs.QueueAttributeNameApproximateNumberOfMessages], attr[AttrKeyQueueName]),
					Value:   attr[AttrKeyQueueUrl].String(),
				})
		}
		if interaction == noInteraction {
			return "", errors.New("no queue found, change --filter")
		}
		err := interact.
			NewInteraction("Ambiguous queue selection...", list...).
			Resolve(&queueURL)
		if err != nil {
			return "", err
		}
	case l == 0:
		return "", errors.New("no queue found, change --filter")
	}
	return queueURL, nil
}
