package main

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const anyLimit = 1000

func Test_a_single_message_can_be_simplified(t *testing.T) {
	awsMsg := sqs.ReceiveMessageOutput{
		Messages: builder().
			addMsg("body1").
			withAttr(kv{k: "k1", v: "v1"}).
			withMsgAttr(kv{k: "msgK1", v: "msgV1"}).
			build(),
	}
	actual := simplifyMessage(&awsMsg)

	require.NotEmpty(t, actual)
	assert.Equal(t, flexiString("body1"), actual[0].Message)
	assert.Equal(t, "v1", actual[0].AwsAttrib["k1"])
	assert.Equal(t, "msgV1", actual[0].CustAttrib["msgK1"])
}

func Test_a_single_message_can_produce_a_summary(t *testing.T) {
	msg := message{
		Message:    "any-message",
		AwsAttrib:  map[string]string{"ak1": "av1"},
		CustAttrib: map[string]string{"mk1": "mv1"},
	}

	sum := summary{}
	sum.addOne(msg)
	sum.analyse(anyLimit)

	assert.Equal(t, 1, len(sum.MsgAttribs["mk1"]))
	assert.Equal(t, 1, sum.MsgAttribs["mk1"]["mv1"])
}

func Test_when_multiple_messages_are_processed(t *testing.T) {
	msg := func(k, v string) message {
		return message{
			CustAttrib: map[string]string{k: v},
		}
	}
	t.Run("with identical keys", func(t *testing.T) {
		t.Run("if the values From a non-unique finite set there is a count of the kv pair for the key", func(t *testing.T) {
			sum := summary{}
			sum.addOne(msg("k1", "v1"))
			sum.addOne(msg("k1", "v1"))
			sum.addOne(msg("k1", "v2"))
			sum.analyse(anyLimit)

			assert.Equal(t, 2, len(sum.MsgAttribs["k1"]))
			assert.Equal(t, 2, sum.MsgAttribs["k1"]["v1"])
			assert.Equal(t, 1, sum.MsgAttribs["k1"]["v2"])
		})
		t.Run("if the number of unique values hits a limit, the values upto the limit and a  marker event is returned", func(t *testing.T) {
			sum := summary{}
			const maxUnique int = 6
			var values []string
			for i := 0; i < maxUnique+1; i++ {
				v := fmt.Sprintf("v%d", i)
				values = append(values, v)
				sum.addOne(msg("k1", v))
			}

			sum.analyse(int64(maxUnique))

			require.Equal(t, 1, len(sum.MsgAttribs))
			assert.Equal(t, maxUnique+1, len(sum.MsgAttribs["k1"]))

			count, ok := sum.MsgAttribs["k1"][KeyNameMaxUnique]
			assert.True(t, ok)
			assert.Equal(t, 0, count)
		})
	})
	t.Run("with unique keys", func(t *testing.T) {
		t.Run("if the values are equal there is a count for the key", func(t *testing.T) {
			sum := summary{}
			sum.addOne(msg("k1", "any-value"))
			sum.addOne(msg("k2", "any-value"))
			sum.addOne(msg("k3", "any-value"))
			sum.analyse(anyLimit)

			assert.Equal(t, 1, len(sum.MsgAttribs["k1"]))
			assert.Equal(t, 1, sum.MsgAttribs["k1"]["any-value"])
			assert.Equal(t, 1, len(sum.MsgAttribs["k2"]))
			assert.Equal(t, 1, sum.MsgAttribs["k2"]["any-value"])
			assert.Equal(t, 1, len(sum.MsgAttribs["k3"]))
			assert.Equal(t, 1, sum.MsgAttribs["k3"]["any-value"])
		})
	})
}

func Test_when_analyse_is_called_string_version_of_timestamps_are_set(t *testing.T) {
	sum := summary{}
	var dtm int64 = 1574154612615
	var dtmStr = "2019-11-19 09:10:12.615"

	sum.addOne(message{
		AwsAttrib: map[string]string{
			"SentTimestamp": "1574154612615",
		},
	})
	sum.analyse(anyLimit)

	assert.Equal(t, dtm, sum.Timestamps["SentTimestamp"].From)
	assert.Equal(t, dtm, sum.Timestamps["SentTimestamp"].To)
	assert.Equal(t, dtmStr, sum.Timestamps["SentTimestamp"].ToStr)
	assert.Equal(t, dtmStr, sum.Timestamps["SentTimestamp"].FromStr)
}

type (
	buildData struct {
		msgs []*sqs.Message
	}
	kv struct{ k, v string }
)

func builder() *buildData {
	return &buildData{}
}

func (b *buildData) withAttr(kvs ...kv) *buildData {
	msg := b.msgs[len(b.msgs)-1]
	msg.Attributes = make(map[string]*string)
	for _, kv := range kvs {
		msg.Attributes[kv.k] = aws.String(kv.v)
	}
	return b
}

func (b *buildData) withMsgAttr(kvs ...kv) *buildData {
	msg := b.msgs[len(b.msgs)-1]
	msg.MessageAttributes = make(map[string]*sqs.MessageAttributeValue)
	for _, kv := range kvs {
		msg.MessageAttributes[kv.k] = msgAttr(kv.v)
	}
	return b
}

func (b *buildData) addMsg(body string) *buildData {
	b.msgs = append(b.msgs, &sqs.Message{Body: &body})
	return b
}

func (b *buildData) build() []*sqs.Message {
	return b.msgs
}

func msgAttr(val string) *sqs.MessageAttributeValue {
	return &sqs.MessageAttributeValue{
		StringValue: aws.String(val),
	}
}
