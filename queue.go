package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	client        *redis.Client
	pendingKey    string
	processingKey string
	dlqKey        string
}

func NewQueue(client *redis.Client, queueName string) *Queue {
	return &Queue{
		client:        client,
		pendingKey:    fmt.Sprintf("queue:%s:pending", queueName),
		processingKey: fmt.Sprintf("queue:%s:processing", queueName),
		dlqKey:        fmt.Sprintf("queue:%s:dlq", queueName),
	}
}

func (q *Queue) Enqueue(ctx context.Context, task *Task) error {
	data, err := task.Marshal()
	if err != nil {
		return fmt.Errorf("failed to serialize task for enqueue: %w", err)
	}

	// LPUSH adds the JSON byte slice to the head (left) of the pending list
	err = q.client.LPush(ctx, q.pendingKey, data).Err()
	if err != nil {
		return fmt.Errorf("redis LPUSH failed: %w", err)
	}
	return nil
}

// DequeueAtomic fetches task safely from pending to processing using atomic BLMOVE.
func (q *Queue) DequeueAtomic(ctx context.Context, timeout time.Duration) (*Task, error) {
	// Move task atomically from RIGHT of pending -> LEFT of processing (FIFO).
	result, err := q.client.BLMove(ctx, q.pendingKey, q.processingKey, "RIGHT", "LEFT", timeout).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("redis BLMOVE failed: %w", err)
	}

	// Unmarshal raw JSON bytes back into a *Task struct
	task, err := Unmarshal([]byte(result))
	if err != nil {
		return nil, fmt.Errorf("failed to parse dequeued task: %w", err)
	}

	return task, nil
}

func (q *Queue) Acknowledge(ctx context.Context, task *Task) error {
	data, err := task.Marshal()
	if err != nil {
		return err
	}
	// LREM removes 1 exact match of this serialized task from processing
	return q.client.LRem(ctx, q.processingKey, 1, data).Err()
}
