package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
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
	flags, err := parseFlags(os.Args[1:])
	if err != nil {
		return err
	}
	if flags.showVersion {
		fmt.Printf("%s %s %s\n", version, commit, date)
		return nil
	}

	if flags.regionArg == "" {
		flags.regionArg = "eu-west-1"
	}
	sess, err := session.NewSession(&aws.Config{Region: aws.String(flags.regionArg)})
	if err != nil {
		return err
	}
	action, err := cmdAction(flags)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	registerForCtrlC(cancel)

	svc := sqs.New(sess)
	result, err := listQueues(listQueueOptions{
		svc:         svc,
		filter:      flags.filter,
		allMessages: flags.allMessages,
		ctx:         ctx,
	})
	if err != nil {
		return err
	}
	if action == CmdActionList {
		return printList(flags.asJson, result)
	}

	queueURL, err := resolveQueueUrl(result, interactionType(flags.noInteraction))
	if err != nil {
		return err
	}

	switch action {
	case CmdActionRead:
		readMessages(readQueueOptions{
			svc:               svc,
			queueURL:          queueURL,
			visibilityTimeout: flags.visibility,
			maxUnique:         flags.maxUnique,
			ctx:               ctx,
			msg:               make(chan []message),
			err:               make(chan error),
			wg:                &sync.WaitGroup{},
		})
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
