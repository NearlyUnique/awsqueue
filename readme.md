# AWS Queue

Simple queue lister, supports flags

* `-filter` : any case insensitive substring for queue name
* `-only-messages` : only display queues containing messages
* `-region` : AWS region, uses env var or eu-west-1

Must be logged on with valid profile

List all non empty dead letter queues
 
```bash
awsqueque -with-messages -filter dlq 
```

## to build

```bash
go build
```