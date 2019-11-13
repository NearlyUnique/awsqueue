package main

import (
	"testing"

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
	assert.Equal(t, "body1", actual[0].Message)
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

	assert.Equal(t, 1, len(sum.msgAttribs["mk1"]))
	assert.Equal(t, 1, sum.msgAttribs["mk1"]["mv1"])
}

func Test_when_multiple_messages_are_processed(t *testing.T) {
	msg := func(k, v string) message {
		return message{
			MsgAttrib: map[string]string{k: v},
		}
	}
	t.Run("with identical keys", func(t *testing.T) {
		t.Run("if the values are equal there is a count of the kv pair for the key", func(t *testing.T) {
			sum := summary{}
			sum.addOne(msg("k1", "v1"))
			sum.addOne(msg("k1", "v1"))
			sum.addOne(msg("k1", "v1"))

			assert.Equal(t, 1, len(sum.msgAttribs["k1"]))
			assert.Equal(t, 3, sum.msgAttribs["k1"]["v1"])
		})
		t.Run("if the values are NOT equal there is a count of the kv pair for the key", func(t *testing.T) {
			sum := summary{}
			sum.addOne(msg("k1", "v1"))
			sum.addOne(msg("k1", "v2"))

			assert.Equal(t, 2, len(sum.msgAttribs["k1"]))
			assert.Equal(t, 1, sum.msgAttribs["k1"]["v1"])
			assert.Equal(t, 1, sum.msgAttribs["k1"]["v2"])
		})
	})
	t.Run("with unique keys", func(t *testing.T) {
		t.Run("if the values are equal there is a count for the key", func(t *testing.T) {
			sum := summary{}
			sum.addOne(msg("k1", "any-value"))
			sum.addOne(msg("k2", "any-value"))
			sum.addOne(msg("k3", "any-value"))

			assert.Equal(t, 1, len(sum.msgAttribs["k1"]))
			assert.Equal(t, 1, sum.msgAttribs["k1"]["any-value"])
			assert.Equal(t, 1, len(sum.msgAttribs["k2"]))
			assert.Equal(t, 1, sum.msgAttribs["k2"]["any-value"])
			assert.Equal(t, 1, len(sum.msgAttribs["k3"]))
			assert.Equal(t, 1, sum.msgAttribs["k3"]["any-value"])
		})
	})
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
