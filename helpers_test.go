package main

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_resolve_single_queue(t *testing.T) {
	type at []map[string]flexiString
	t.Run("single result resolved that does not match filter will fail", func(t *testing.T) {
		result := QueueSearchResult{
			Filter: "",
			Attrs: at{
				{AttrKeyQueueName: "one", AttrKeyQueueUrl: "http://any.com/1"},
			},
		}
		queueUrl, err := resolveQueueUrl(result, noInteraction)

		assert.Error(t, err)
		assert.Equal(t, "", queueUrl)
	})
	t.Run("multiple result and no filter does not resolve", func(t *testing.T) {
		result := QueueSearchResult{
			Filter: "",
			Attrs: at{
				{AttrKeyQueueName: "one", AttrKeyQueueUrl: "http://any.com/1"},
				{AttrKeyQueueName: "two", AttrKeyQueueUrl: "http://any.com/2"},
			},
		}
		queueUrl, err := resolveQueueUrl(result, noInteraction)

		assert.Error(t, err)
		assert.Equal(t, "", queueUrl)
	})
	t.Run("multiple result and exact match filter resolves", func(t *testing.T) {
		result := QueueSearchResult{
			Filter:      "one",
			AllMessages: true,
			Attrs: at{
				{AttrKeyQueueName: "oneTwo", AttrKeyQueueUrl: "http://any.com/1"},
				{AttrKeyQueueName: "one", AttrKeyQueueUrl: "http://any.com/2"},
			},
		}
		queueUrl, err := resolveQueueUrl(result, noInteraction)

		assert.NoError(t, err)
		assert.Equal(t, "http://any.com/2", queueUrl)
	})
	t.Run("multiple result with all-Messages=false and one match with Messages", func(t *testing.T) {
		result := QueueSearchResult{
			Filter:      "submatch",
			AllMessages: false,
			Attrs: at{
				{AttrKeyQueueName: "xsubmatch1", AttrKeyQueueUrl: "http://any.com/name1", sqs.QueueAttributeNameApproximateNumberOfMessages: "1"},
				{AttrKeyQueueName: "xsubmatch2", AttrKeyQueueUrl: "http://any.com/name2", sqs.QueueAttributeNameApproximateNumberOfMessages: "0"},
			},
		}
		queueUrl, err := resolveQueueUrl(result, noInteraction)

		assert.NoError(t, err)
		assert.Equal(t, "http://any.com/name1", queueUrl)
	})
}

func Test_command_action_is_determined_from_flags(t *testing.T) {
	t.Run("when no flags, default is list", func(t *testing.T) {
		fs, err := parseFlags([]string{})
		require.NoError(t, err)

		cmd, err := cmdAction(fs)

		require.NoError(t, err)
		assert.Equal(t, CmdActionList, cmd)
	})
	t.Run("when --read, resolved To read", func(t *testing.T) {
		fs, err := parseFlags([]string{"any", "--read"})
		require.NoError(t, err)

		cmd, err := cmdAction(fs)

		require.NoError(t, err)
		assert.Equal(t, CmdActionRead, cmd)
	})
	t.Run("when --write-source, resolved To write", func(t *testing.T) {
		fs, err := parseFlags([]string{"any", "--write-source=any"})
		require.NoError(t, err)

		cmd, err := cmdAction(fs)

		require.NoError(t, err)
		assert.Equal(t, CmdActionWrite, cmd)
	})
	t.Run("attempts To read and write generate error", func(t *testing.T) {
		fs, err := parseFlags([]string{"any", "--write-source=any", "--read"})
		require.NoError(t, err)

		_, err = cmdAction(fs)

		require.Error(t, err)
	})
}

func Test_flexistring_json_marshalling(t *testing.T) {
	type testType struct {
		Value flexiString
	}
	t.Run("a basic string comes out as text", func(t *testing.T) {
		fs := testType{`some text`}

		buf, err := json.Marshal(fs)

		require.NoError(t, err)
		assert.Equal(t, string(buf), `{"Value":"some text"}`)
	})
	t.Run("when the string looks like a json object it is treated as such", func(t *testing.T) {
		fs := testType{`{"some":true}`}

		buf, err := json.Marshal(fs)

		require.NoError(t, err)
		assert.Equal(t, string(buf), `{"Value":{"some":true}}`)
	})
}
