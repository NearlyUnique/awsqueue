# AWS Queue

Simple queue lister, supports flags

* `--filter` : any case insensitive substring for queue name
* `--all` : only display queues containing messages
* `--region` : AWS region, uses env var or eu-west-1

Must be logged on with valid profile

List all non empty dead letter queues
 
```bash
$ awsqueue --help
Usage of default:
      --all                   If true shows message attributes event when there are no messages in the queue
  -f, --filter string         substring search to filter queues
  -j, --json                  Output format defaults to summary (count,name), asJson fives fuller output
      --read                  read messages and meta data, will only run if a single queue can be resolved via --filter
      --region string         AWS region, defaults from env variable (AWS_REGION) then to eu-west-1
      --write-source string   json source file to send messages, will only run if a single queue can be resolved via --filter 
```

## to build

```bash
go build
```