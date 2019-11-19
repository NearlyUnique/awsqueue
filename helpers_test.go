package main

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_resolve_single_queue(t *testing.T) {
	type at []map[string]string
	t.Run("single result resolved", func(t *testing.T) {
		result := QueueSearchResult{
			Filter: "",
			Attrs: at{
				{AttrKeyQueueName: "one", AttrKeyQueueUrl: "http://any.com/1"},
			},
		}
		ok, queueUrl := canResolveSingleQueue(result)

		assert.True(t, ok)
		assert.Equal(t, "http://any.com/1", queueUrl)
	})
	t.Run("multiple result and no filter does not resolve", func(t *testing.T) {
		result := QueueSearchResult{
			Filter: "",
			Attrs: at{
				{AttrKeyQueueName: "one", AttrKeyQueueUrl: "http://any.com/1"},
				{AttrKeyQueueName: "two", AttrKeyQueueUrl: "http://any.com/2"},
			},
		}
		ok, queueUrl := canResolveSingleQueue(result)

		assert.False(t, ok)
		assert.Equal(t, "", queueUrl)
	})
	t.Run("multiple result and exact match filter resolves", func(t *testing.T) {
		result := QueueSearchResult{
			Filter: "one",
			Attrs: at{
				{AttrKeyQueueName: "oneTwo", AttrKeyQueueUrl: "http://any.com/1"},
				{AttrKeyQueueName: "one", AttrKeyQueueUrl: "http://any.com/2"},
			},
		}
		ok, queueUrl := canResolveSingleQueue(result)

		assert.True(t, ok)
		assert.Equal(t, "http://any.com/2", queueUrl)
	})
}

func Test_command_action_is_determined_from_flags(t *testing.T) {
	createFlagSet := func() *pflag.FlagSet {
		fs := pflag.NewFlagSet("any", pflag.ContinueOnError)
		fs.Bool("read", false, "")
		fs.String("write-source", "", "")
		return fs
	}

	t.Run("when no flags, default is list", func(t *testing.T) {
		fs := createFlagSet()
		require.NoError(t, fs.Parse([]string{}))

		cmd, err := cmdAction(fs)

		require.NoError(t, err)
		assert.Equal(t, CmdActionList, cmd)
	})
	t.Run("when --read, resolved to read", func(t *testing.T) {
		fs := createFlagSet()
		require.NoError(t, fs.Parse([]string{"any", "--read"}))

		cmd, err := cmdAction(fs)

		require.NoError(t, err)
		assert.Equal(t, CmdActionRead, cmd)
	})
	t.Run("when --write-source, resolved to write", func(t *testing.T) {
		fs := createFlagSet()
		require.NoError(t, fs.Parse([]string{"any", "--write-source=any"}))

		cmd, err := cmdAction(fs)

		require.NoError(t, err)
		assert.Equal(t, CmdActionWrite, cmd)
	})
	t.Run("attempts to read and write generate error", func(t *testing.T) {
		fs := createFlagSet()
		require.NoError(t, fs.Parse([]string{"any", "--write-source=any", "--read"}))

		_, err := cmdAction(fs)

		require.Error(t, err)
	})
}