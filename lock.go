package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var unlockScript = redis.NewScript(`
	if redis.call("get, KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
`)

type Lock struct {
	client *redis.Client
	key    string
	token  string
	ttl    time.Duration
}

func NewLock(client *redis.Client, taskID, workerToken string, ttl time.Duration) *Lock {
	return &Lock{
		client: client,
		key:    fmt.Sprintf("lock:task:%s", taskID),
		token:  workerToken,
		ttl:    ttl,
	}
}

// Acquire attempts to set the lock key if it does not already exist (SET NX PX).
func (l *Lock) Acquire(ctx context.Context) (bool, error) {
	// SetNX = SET key token NX PX ttl (Atomic check-and-set)
	ok, err := l.client.SetNX(ctx, l.key, l.token, l.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("failed to acquire lock for key %s: %w", l.key, err)
	}
	return ok, nil
}

// Release safely releases the lock using an atomic Lua script to prevent deleting another worker's lock.
func (l *Lock) Release(ctx context.Context) error {
	res, err := unlockScript.Run(ctx, l.client, []string{l.key}, l.token).Int64()
	if err != nil {
		return fmt.Errorf("failed executing release lua script: %w", err)
	}
	if res == 0 {
		// Lock was already expired or stolen by another process
		return fmt.Errorf("lock release failed: token mismatch or lock expired")
	}
	return nil
}

// StartWatchdog runs a background ticker goroutine that renews the TTL periodically while the task is executing.
func (l *Lock) StartWatchdog(ctx context.Context, stopChan <-chan struct{}) {
	ticker := time.NewTicker(l.ttl / 2) // Renew at half-life of TTL
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return // Task finished execution, stop extending lease
		case <-ctx.Done():
			return // Worker context canceled (shutdown)
		case <-ticker.C:
			// Extend TTL atomically using PEXPIRE if token still matches
			renewScript := redis.NewScript(`
				if redis.call("get", KEYS[1]) == ARGV[1] then
					return redis.call("pexpire", KEYS[1], ARGV[2])
				else
					return 0
				end
			`)
			renewScript.Run(ctx, l.client, []string{l.key}, l.token, l.ttl.Milliseconds())
		}
	}
}
