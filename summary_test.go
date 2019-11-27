package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	assert.Equal(t, "v1", actual[0].Attrib["k1"])
	assert.Equal(t, "msgV1", actual[0].MsgAttrib["msgK1"])
}

func Test_a_single_message_can_produce_a_summary(t *testing.T) {
	msg := message{
		Message:   "any-message",
		Attrib:    map[string]string{"ak1": "av1"},
		MsgAttrib: map[string]string{"mk1": "mv1"},
	}

	sum := summary{}
	sum.addOne(msg)
	sum.analyse()

	assert.Equal(t, 1, len(sum.MsgAttribs["mk1"]))
	assert.Equal(t, 1, sum.MsgAttribs["mk1"]["mv1"])
}

func Test_when_multiple_messages_are_processed(t *testing.T) {
	msg := func(k, v string) message {
		return message{
			MsgAttrib: map[string]string{k: v},
		}
	}
	t.Run("with identical keys", func(t *testing.T) {
		t.Run("if the values From a non-unique finite set there is a count of the kv pair for the key", func(t *testing.T) {
			sum := summary{}
			sum.addOne(msg("k1", "v1"))
			sum.addOne(msg("k1", "v1"))
			sum.addOne(msg("k1", "v2"))
			sum.analyse()

			assert.Equal(t, 2, len(sum.MsgAttribs["k1"]))
			assert.Equal(t, 2, sum.MsgAttribs["k1"]["v1"])
			assert.Equal(t, 1, sum.MsgAttribs["k1"]["v2"])
		})
		t.Run("if the number of unique values hits a limit a single marker event is returned", func(t *testing.T) {
			sum := summary{}
			var values []string
			for i := 0; i < 6; i++ {
				v := fmt.Sprintf("v%d", i)
				values = append(values, v)
				sum.addOne(msg("k1", v))
			}

			sum.analyse()

			assert.Equal(t, 1, len(sum.MsgAttribs))
			assert.Equal(t, 1, len(sum.MsgAttribs["k1"]))
			var part []string
			actualCount := 0
			for k, v := range sum.MsgAttribs["k1"] {
				part = strings.Split(k, ":")
				actualCount = v
				break
			}
			require.Equal(t, 2, len(part))
			assert.Equal(t, "$UNIQUE", part[0])
			assert.Contains(t, values, part[1])
			assert.Equal(t, 6, actualCount)
		})
	})
	t.Run("with unique keys", func(t *testing.T) {
		t.Run("if the values are equal there is a count for the key", func(t *testing.T) {
			sum := summary{}
			sum.addOne(msg("k1", "any-value"))
			sum.addOne(msg("k2", "any-value"))
			sum.addOne(msg("k3", "any-value"))
			sum.analyse()

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
	var dtmStr = "2019-11-19T09:10:12.615"

	sum.addOne(message{
		Attrib: map[string]string{
			"SentTimestamp": "1574154612615",
		},
	})
	sum.analyse()

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
		msg.Attributes[kv.k] = pstring(kv.v)
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
		StringValue: pstring(val),
	}
}

func pstring(s string) *string {
	return &s
}

func unixSec(t time.Time) int64 {
	return t.UnixNano() / 1000000
}
