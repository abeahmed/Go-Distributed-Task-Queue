package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
)

func main() {
	log.Println("Initializing gopher-queue engine...")

	// 1. Initialize Redis connection pool
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	// 2. Initialize Queue and Worker Pool
	queue := NewQueue(rdb, "main")
	pool := NewWorkerPool(queue, rdb, 5, "worker-node-01")

	// 3. Register Core Handlers
	RegisterDefaultHandlers(pool)

	// 4. Launch Worker Engine
	pool.Start(ctx)

	// 5. Optionally run sample producer if flag/env is set
	if os.Getenv("SEED_DEMO_DATA") == "true" {
		go SeedSampleTasks(ctx, queue)
	}

	log.Println("gopher-queue is running. Press Ctrl+C to shut down...")
	<-ctx.Done()

	log.Println("\nShutdown signal received! Draining worker pool...")
	pool.Stop()
	log.Println("gopher-queue terminated cleanly.")
}
