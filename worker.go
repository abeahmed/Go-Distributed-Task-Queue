package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type TaskHandler func(payload []byte) error

type WorkerPool struct {
	queue       *Queue
	client      *redis.Client
	concurrency int
	handlers    map[string]TaskHandler
	workerToken string
	lockTTL     time.Duration
	wg          sync.WaitGroup
}

func NewWorkerPool(queue *Queue, client *redis.Client, concurrency int, workerToken string) *WorkerPool {
	return &WorkerPool{
		queue:       queue,
		client:      client,
		concurrency: concurrency,
		handlers:    make(map[string]TaskHandler),
		workerToken: workerToken,
		lockTTL:     10 * time.Second,
	}
}

func (wp *WorkerPool) RegisterHandler(taskType string, handler TaskHandler) {
	wp.handlers[taskType] = handler
}

func (wp *WorkerPool) Start(ctx context.Context) {
	log.Printf("Starting worker pool with %d concurrent workers...", wp.concurrency)

	for i := 1; i <= wp.concurrency; i++ {
		wp.wg.Add(1)
		go wp.workerLoop(ctx, i)
	}
}

func (wp *WorkerPool) Stop() {
	wp.wg.Wait()
	log.Println("All workers shut down cleanly.")
}

func (wp *WorkerPool) workerLoop(ctx context.Context, workerID int) {
	defer wp.wg.Done()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d received shutdown signal, exiting loop.", workerID)
			return
		default:
			// 1. Pop a task atomically from Redis (blocking wait up to 2 seconds)
			task, err := wp.queue.DequeueAtomic(ctx, 2*time.Second)
			if err != nil {
				log.Printf("Worker %d dequeue error: %v", workerID, err)
				continue
			}
			if task == nil {
				// No tasks in queue; loop back and wait again
				continue
			}

			// 2. Execute the task with locks and watchdogs
			wp.processTask(ctx, workerID, task)
		}
	}
}

func (wp *WorkerPool) processTask(ctx context.Context, workerID int, task *Task) {
	handler, exists := wp.handlers[task.Type]
	if !exists {
		log.Printf("Worker %d: No handler registered for task type '%s'", workerID, task.Type)
		return
	}

	// Try to acquire distributed lock for mutual exclusion
	lock := NewLock(wp.client, task.ID, wp.workerToken, wp.lockTTL)
	acquired, err := lock.Acquire(ctx)
	if err != nil || !acquired {
		log.Printf("Worker %d: Could not acquire lock for task %s (already being executed elsewhere)", workerID, task.ID)
		return
	}

	// Spin up background watchdog heartbeat
	stopChan := make(chan struct{})
	go lock.StartWatchdog(ctx, stopChan)

	// Execute task handler
	err = handler(task.Payload)

	// JOB DONE: Send signal to stopChan by closing it
	close(stopChan)

	// Release lock
	lock.Release(ctx)

	if err != nil {
		log.Printf("Worker %d: Task %s failed: %v", workerID, task.ID, err)
		wp.handleFailure(ctx, task, err)
		return
	}

	// Acknowledge task (removes from processing list)
	if err := wp.queue.Acknowledge(ctx, task); err != nil {
		log.Printf("Worker %d: Failed to acknowledge task %s: %v", workerID, task.ID, err)
	} else {
		log.Printf("Worker %d: Successfully completed task %s", workerID, task.ID)
	}
}

func (wp *WorkerPool) handleFailure(ctx context.Context, task *Task, taskErr error) {
	task.RetryCount++
	task.Error = taskErr.Error()

	if task.RetryCount > task.MaxRetries {
		log.Printf("Task %s reached max retries (%d). Moving to Dead Letter Queue.", task.ID, task.MaxRetries)
		// Move to DLQ and cleanup processing list
		wp.queue.Acknowledge(ctx, task)
	} else {
		log.Printf("Task %s failed (Attempt %d/%d). Re-enqueueing...", task.ID, task.RetryCount, task.MaxRetries)
		// Exponential backoff: wait 2^RetryCount seconds before re-enqueueing
		time.Sleep(time.Duration(1<<task.RetryCount) * time.Second)
		wp.queue.Enqueue(ctx, task)
		wp.queue.Acknowledge(ctx, task)
	}
}
