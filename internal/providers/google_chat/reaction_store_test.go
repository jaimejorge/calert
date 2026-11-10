package google_chat

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testReactionTTL = time.Hour

// newTestReactionStore returns the named store backend together with a
// function that moves its clock forward.
func newTestReactionStore(t *testing.T, backend string) (reactionStore, func(time.Duration)) {
	t.Helper()
	if backend == "redis" {
		mr := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		return newRedisReactionStore(client, "calert", testReactionTTL), mr.FastForward
	}
	now := time.Now()
	store := newMemoryReactionStore(testReactionTTL)
	store.now = func() time.Time { return now }
	return store, func(d time.Duration) { now = now.Add(d) }
}

func TestReactionStore(t *testing.T) {
	for _, backend := range reactionBackends() {
		t.Run(backend, func(t *testing.T) { testReactionStore(t, backend) })
	}
}

func testReactionStore(t *testing.T, backend string) {
	ctx := context.Background()
	const (
		first  = "spaces/AAAA/messages/first"
		second = "spaces/AAAA/messages/second"
	)

	t.Run("unknown key has no record", func(t *testing.T) {
		store, _ := newTestReactionStore(t, backend)

		_, ok, err := store.get(ctx, "missing")

		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("the first claimed message stays the original", func(t *testing.T) {
		store, _ := newTestReactionStore(t, backend)

		record, err := store.claim(ctx, "key", first)
		require.NoError(t, err)
		assert.Equal(t, reactionRecord{Message: first}, record)

		record, err = store.claim(ctx, "key", second)
		require.NoError(t, err)
		assert.Equal(t, first, record.Message, "a duplicate FIRING must not replace the original message")

		other, err := store.claim(ctx, "other-key", second)
		require.NoError(t, err)
		assert.Equal(t, second, other.Message, "keys are independent")
	})

	t.Run("updates accumulate instead of overwriting each other", func(t *testing.T) {
		store, _ := newTestReactionStore(t, backend)
		_, err := store.claim(ctx, "key", first)
		require.NoError(t, err)

		record, err := store.setFiringReaction(ctx, "key", first+"/reactions/r1")
		require.NoError(t, err)
		assert.Equal(t, reactionRecord{Message: first, FiringReaction: first + "/reactions/r1"}, record)

		record, err = store.markResolved(ctx, "key")
		require.NoError(t, err)
		assert.Equal(t, reactionRecord{Message: first, FiringReaction: first + "/reactions/r1", Resolved: true}, record)

		// A firing reaction recorded after the alert resolved is still
		// reported, so that the caller can remove it.
		record, err = store.setFiringReaction(ctx, "key", first+"/reactions/r2")
		require.NoError(t, err)
		assert.True(t, record.Resolved)
		assert.Equal(t, first+"/reactions/r2", record.FiringReaction)

		stored, ok, err := store.get(ctx, "key")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, record, stored)

		record, err = store.claim(ctx, "key", second)
		require.NoError(t, err)
		assert.Equal(t, stored, record, "a late FIRING duplicate sees the resolved record")
	})

	t.Run("a record without an original message is not usable", func(t *testing.T) {
		store, _ := newTestReactionStore(t, backend)
		_, err := store.markResolved(ctx, "key")
		require.NoError(t, err)

		_, ok, err := store.get(ctx, "key")

		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("records expire after the ttl", func(t *testing.T) {
		store, advance := newTestReactionStore(t, backend)
		_, err := store.claim(ctx, "key", first)
		require.NoError(t, err)

		advance(testReactionTTL - time.Minute)
		_, ok, err := store.get(ctx, "key")
		require.NoError(t, err)
		require.True(t, ok)

		advance(2 * time.Minute)
		_, ok, err = store.get(ctx, "key")
		require.NoError(t, err)
		assert.False(t, ok, "reading a record must not extend its retention")

		record, err := store.claim(ctx, "key", second)
		require.NoError(t, err)
		assert.Equal(t, second, record.Message, "an expired record can be claimed again")
	})

	t.Run("a repeated FIRING keeps a long-running alert alive", func(t *testing.T) {
		store, advance := newTestReactionStore(t, backend)
		_, err := store.claim(ctx, "key", first)
		require.NoError(t, err)

		for range 5 {
			advance(testReactionTTL - time.Minute)
			record, err := store.claim(ctx, "key", second)
			require.NoError(t, err)
			require.Equal(t, first, record.Message)
		}
	})
}

func TestMemoryReactionStoreSweepsExpiredRecords(t *testing.T) {
	now := time.Now()
	store := newMemoryReactionStore(testReactionTTL)
	store.now = func() time.Time { return now }
	ctx := context.Background()

	for _, key := range []string{"a", "b", "c"} {
		_, err := store.claim(ctx, key, "spaces/AAAA/messages/"+key)
		require.NoError(t, err)
	}
	now = now.Add(2 * testReactionTTL)
	_, err := store.claim(ctx, "d", "spaces/AAAA/messages/d")
	require.NoError(t, err)

	assert.Len(t, store.records, 1, "alerts that never resolve must not accumulate forever")
}

func TestRedisReactionStoreKeys(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	startsAt := time.Date(2026, 6, 12, 9, 0, 0, 0, time.UTC)
	key := threadKeyForAlert("room", `fingerprint="sensitive"`, startsAt)

	store := newRedisReactionStore(client, "", testReactionTTL)
	_, err := store.claim(ctx, key, "spaces/AAAA/messages/first")
	require.NoError(t, err)

	keys := mr.Keys()
	require.Len(t, keys, 1)
	assert.Regexp(t, `^calert:reaction:[a-f0-9]{64}$`, keys[0], "keys are bounded digests under the default prefix")
	assert.Equal(t, testReactionTTL, mr.TTL(keys[0]))

	prefixed := newRedisReactionStore(client, "team-a", testReactionTTL)
	_, ok, err := prefixed.get(ctx, key)
	require.NoError(t, err)
	assert.False(t, ok, "a different key prefix is a different namespace")
	assert.Equal(t, "team-a:reaction:"+key, prefixed.redisKey(key))
	assert.NotEqual(t, key, threadKeyForAlert("other-room", `fingerprint="sensitive"`, startsAt), "rooms never share reaction state")
}
