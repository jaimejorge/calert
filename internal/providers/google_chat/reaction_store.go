package google_chat

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// reactionRecord is what calert remembers about the first FIRING message of
// an alert lifecycle, so that a later RESOLVED notification can find it.
type reactionRecord struct {
	// Message is the Google Chat resource name (spaces/*/messages/*) of the
	// original FIRING notification.
	Message string
	// FiringReaction is the resource name of the reaction added on FIRING.
	// It is empty until that reaction has been created.
	FiringReaction string
	// Resolved is set once the RESOLVED reaction has been applied.
	Resolved bool
}

// reactionStore maps an alert lifecycle key (see threadKeyForAlert) to its
// reactionRecord. Every operation is atomic and only ever adds information,
// so replicas handling the same alert concurrently cannot erase each other's
// writes; they converge on the same record instead.
type reactionStore interface {
	// claim records message as the original notification for key unless one
	// is already stored, and returns the stored record. It also extends the
	// record's retention, so a still-firing alert is never forgotten.
	claim(ctx context.Context, key, message string) (reactionRecord, error)
	// get returns the record for key, if there is one.
	get(ctx context.Context, key string) (reactionRecord, bool, error)
	// setFiringReaction stores the firing reaction's resource name and
	// returns the record as it is after the update.
	setFiringReaction(ctx context.Context, key, reaction string) (reactionRecord, error)
	// markResolved flags the record as resolved and returns the record as it
	// is after the update.
	markResolved(ctx context.Context, key string) (reactionRecord, error)
}

type memoryReaction struct {
	record  reactionRecord
	expires time.Time
}

// memoryReactionStore keeps reaction state in process memory. State is lost
// on restart and is not shared between replicas.
type memoryReactionStore struct {
	ttl time.Duration
	now func() time.Time

	sync.Mutex
	records   map[string]memoryReaction
	nextSweep time.Time
}

func newMemoryReactionStore(ttl time.Duration) *memoryReactionStore {
	return &memoryReactionStore{ttl: ttl, now: time.Now, records: make(map[string]memoryReaction)}
}

// load returns the live record for key. Callers must hold the lock.
func (s *memoryReactionStore) load(key string, now time.Time) (reactionRecord, bool) {
	entry, ok := s.records[key]
	if !ok || !now.Before(entry.expires) {
		delete(s.records, key)
		return reactionRecord{}, false
	}
	return entry.record, true
}

// sweep drops expired records at most once per minute so alerts that never
// resolve cannot grow the map without bound. Callers must hold the lock.
func (s *memoryReactionStore) sweep(now time.Time) {
	if now.Before(s.nextSweep) {
		return
	}
	s.nextSweep = now.Add(time.Minute)
	for key, entry := range s.records {
		if !now.Before(entry.expires) {
			delete(s.records, key)
		}
	}
}

func (s *memoryReactionStore) update(key string, change func(*reactionRecord)) reactionRecord {
	s.Lock()
	defer s.Unlock()

	now := s.now()
	s.sweep(now)
	record, _ := s.load(key, now)
	change(&record)
	s.records[key] = memoryReaction{record: record, expires: now.Add(s.ttl)}
	return record
}

func (s *memoryReactionStore) claim(_ context.Context, key, message string) (reactionRecord, error) {
	return s.update(key, func(r *reactionRecord) {
		if r.Message == "" {
			r.Message = message
		}
	}), nil
}

func (s *memoryReactionStore) get(_ context.Context, key string) (reactionRecord, bool, error) {
	s.Lock()
	defer s.Unlock()
	record, ok := s.load(key, s.now())
	return record, ok && record.Message != "", nil
}

func (s *memoryReactionStore) setFiringReaction(_ context.Context, key, reaction string) (reactionRecord, error) {
	return s.update(key, func(r *reactionRecord) { r.FiringReaction = reaction }), nil
}

func (s *memoryReactionStore) markResolved(_ context.Context, key string) (reactionRecord, error) {
	return s.update(key, func(r *reactionRecord) { r.Resolved = true }), nil
}

const (
	reactionFieldMessage  = "message"
	reactionFieldFiring   = "firing_reaction"
	reactionFieldResolved = "resolved"
)

// redisReactionStore shares reaction state between calert replicas. Each
// record is a Redis hash, so the fields written by different replicas never
// overwrite one another.
type redisReactionStore struct {
	client redis.UniversalClient
	prefix string
	ttl    time.Duration
}

func newRedisReactionStore(client redis.UniversalClient, prefix string, ttl time.Duration) *redisReactionStore {
	return &redisReactionStore{client: client, prefix: prefix, ttl: ttl}
}

// redisKey namespaces key, which is already a room-scoped SHA-256 digest.
func (r *redisReactionStore) redisKey(key string) string {
	prefix := r.prefix
	if prefix == "" {
		prefix = "calert"
	}
	return prefix + ":reaction:" + key
}

// apply runs write and then reads the record back in one MULTI/EXEC
// transaction, refreshing the record's retention.
func (r *redisReactionStore) apply(ctx context.Context, key string, write func(redis.Pipeliner, string)) (reactionRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	redisKey := r.redisKey(key)
	var fields *redis.SliceCmd
	_, err := r.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		if write != nil {
			write(pipe, redisKey)
			pipe.PExpire(ctx, redisKey, r.ttl)
		}
		fields = pipe.HMGet(ctx, redisKey, reactionFieldMessage, reactionFieldFiring, reactionFieldResolved)
		return nil
	})
	if err != nil {
		return reactionRecord{}, fmt.Errorf("updating reaction state: %w", err)
	}

	values := fields.Val()
	if len(values) != 3 {
		return reactionRecord{}, fmt.Errorf("decoding reaction state")
	}
	message, _ := values[0].(string)
	firing, _ := values[1].(string)
	resolved, _ := values[2].(string)
	return reactionRecord{Message: message, FiringReaction: firing, Resolved: resolved != ""}, nil
}

func (r *redisReactionStore) claim(ctx context.Context, key, message string) (reactionRecord, error) {
	return r.apply(ctx, key, func(pipe redis.Pipeliner, redisKey string) {
		pipe.HSetNX(ctx, redisKey, reactionFieldMessage, message)
	})
}

func (r *redisReactionStore) get(ctx context.Context, key string) (reactionRecord, bool, error) {
	record, err := r.apply(ctx, key, nil)
	return record, err == nil && record.Message != "", err
}

func (r *redisReactionStore) setFiringReaction(ctx context.Context, key, reaction string) (reactionRecord, error) {
	return r.apply(ctx, key, func(pipe redis.Pipeliner, redisKey string) {
		pipe.HSet(ctx, redisKey, reactionFieldFiring, reaction)
	})
}

func (r *redisReactionStore) markResolved(ctx context.Context, key string) (reactionRecord, error) {
	return r.apply(ctx, key, func(pipe redis.Pipeliner, redisKey string) {
		pipe.HSet(ctx, redisKey, reactionFieldResolved, "1")
	})
}
