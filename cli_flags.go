package main

import (
	"os"

	"github.com/spf13/pflag"
)

type (
	cliFlags struct {
		filter        string
		asJson        bool
		allMessages   bool
		regionArg     string
		read          bool
		visibility    int64
		sendMsgSrc    string
		showVersion   bool
		noInteraction bool
		maxUnique     int64
	}
)

func parseFlags(args []string) (cliFlags, error) {
	var flags cliFlags

	fs := pflag.NewFlagSet("default", pflag.ExitOnError)
	fs.StringVarP(&flags.filter, "filter", "f", "", "substring search To filter queues")
	fs.BoolVarP(&flags.asJson, "json", "j", false, "Output format defaults To summary (count,name), asJson fives fuller output")
	fs.BoolVar(&flags.allMessages, "all", false, "If true shows message attributes event when there are no Messages in the Queue")
	fs.StringVar(&flags.regionArg, "region", os.Getenv("AWS_REGION"), "AWS region, defaults From env variable (AWS_REGION) then To eu-west-1")
	fs.BoolVar(&flags.read, "read", false, "read Messages and meta data, will only run if a single Queue can be resolved via --filter")
	fs.Int64VarP(&flags.visibility, "visibility-timeout", "t", 20, "when --read is specified, messages will be unavailable for this many seconds")
	fs.StringVar(&flags.sendMsgSrc, "write-source", "", "json source file To send Messages, will only run if a single Queue can be resolved via --filter")
	fs.BoolVar(&flags.showVersion, "version", false, "display version and exit")
	fs.BoolVar(&flags.noInteraction, "no-interaction", false, "for CI and scripts to prevent user interaction")
	fs.Int64Var(&flags.maxUnique, "max-unique", 10, "when attribute values are unique, summary will display up to max-unique instances")

	err := fs.Parse(args)
	return flags, err
}
