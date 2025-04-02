package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	TaskQueueName       = "jobrunner:tasks"
	DeadLetterQueueName = "jobrunner:tasks:dead"
)

// RedisClient implements Client using Redis
type RedisClient struct {
	client            *redis.Client
	heartbeatInterval time.Duration
	started           bool
}

// NewRedisClient creates a new Redis queue client
func NewRedisClient(addr, password string, db int, heartbeatInterval int64) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, err
	}

	return &RedisClient{
		client:            client,
		heartbeatInterval: time.Duration(heartbeatInterval) * time.Second,
		started:           false,
	}, nil
}

// Publish sends a task message to the queue
func (r *RedisClient) Publish(ctx context.Context, message TaskMessage) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return r.client.RPush(ctx, TaskQueueName, data).Err()
}

// Subscribe starts listening for messages and processes them with the handler. One client can
// only be subscribed once
func (r *RedisClient) Subscribe(ctx context.Context, handler func(TaskMessage) error) error {
	if r.started {
		return errors.New("redis client already subscribed")
	}

	// Start heartbeat goroutine
	heartbeatCtx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		r.started = false
	}()

	workerID := uuid.New().String()
	processingKey := fmt.Sprintf("jobrunner:processing:%s", workerID)
	r.started = true

	go r.sendHeartbeat(heartbeatCtx, workerID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// BLPOP with timeout ensures we can check context cancellation
			result, err := r.client.BLPop(ctx, 1*time.Second, TaskQueueName).Result()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					// No message available, continue
					continue
				}
				return err
			}

			if len(result) < 2 {
				continue
			}

			messageData := []byte(result[1])
			var message TaskMessage
			if err := json.Unmarshal(messageData, &message); err != nil {
				// Log error but continue processing
				continue
			}

			// Track that this task is being processed
			// Store with TTL of 2x heartbeat interval
			pipe := r.client.Pipeline()
			pipe.HSet(ctx, processingKey, message.ExecutionID, messageData)
			pipe.Expire(ctx, processingKey, r.heartbeatInterval*3)
			if _, err := pipe.Exec(ctx); err != nil {
				log.Error().Err(err).Msg("Failed to track processing task")
				// Return message to queue
				r.client.RPush(ctx, TaskQueueName, messageData)
				continue
			}

			// Process message
			err = processMessage(handler, message)

			// Remove from processing list regardless of success/failure
			r.client.HDel(ctx, processingKey, fmt.Sprintf("%d", message.ExecutionID))

			if err != nil {
				// On failure, send to dead letter queue with metadata
				deadMsg := map[string]interface{}{
					"message":   message,
					"error":     err.Error(),
					"timestamp": time.Now(),
					"worker_id": workerID,
				}
				deadData, _ := json.Marshal(deadMsg)
				r.client.RPush(ctx, DeadLetterQueueName, deadData)
				log.Error().
					Err(err).
					Int64("execution_id", message.ExecutionID).
					Msg("Task failed, sent to dead letter queue")
			}
		}
	}
}

func processMessage(handler func(TaskMessage) error, message TaskMessage) (err error) {
	defer func() {
		if rcv := recover(); rcv != nil {
			// Log the panic
			log.Error().Interface("panic", rcv).Int64("execution_id", message.ExecutionID).Msg("Handler panicked")

			err = fmt.Errorf("handler panicked: %v", rcv)
		}
	}()

	return handler(message)
}

// sendHeartBeat periodically update TTL on processing tasks. This ensures that RecoverStaleTasks
// does not pick up the tasks owned by this worker
func (r *RedisClient) sendHeartbeat(ctx context.Context, workerID string) {
	processingKey := fmt.Sprintf("jobrunner:processing:%s", workerID)
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.client.Expire(ctx, processingKey, r.heartbeatInterval*5)
		}
	}
}

// RecoverStaleTasks recovers stale tasks from workers that have died. This is a global operation.
// Still WIP
func (r *RedisClient) RecoverStaleTasks(ctx context.Context) {
	ticker := time.NewTicker(r.heartbeatInterval * 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Find all processing lists
			processingKeys, err := r.client.Keys(ctx, "jobrunner:processing:*").Result()
			if err != nil {
				log.Error().Err(err).Msg("Failed to scan for processing keys")
				continue
			}

			for _, key := range processingKeys {
				ttl, err := r.client.TTL(ctx, key).Result()
				if err != nil {
					log.Error().Err(err).Str("key", key).Msg("Failed to get TTL for processing key")
					continue
				}

				// Key has no expiry
				if ttl == -1 {
					continue
				}

				// A key is considered stale if:
				// 1. TTL is very low (less than half the heartbeat interval)
				// 2. OR it will expire soon relative to our recovery cycle (2 * heartbeat interval)
				// 3. The key "no longer exists" (expired)
				staleThreshold := r.heartbeatInterval.Seconds() / 2
				if ttl.Seconds() < staleThreshold || ttl == -2 {
					log.Info().
						Str("key", key).
						Float64("ttl_seconds", ttl.Seconds()).
						Float64("threshold", staleThreshold).
						Msg("Found stale worker with low TTL")

					// Get all tasks from this worker
					tasks, err := r.client.HGetAll(ctx, key).Result()
					if err != nil {
						log.Error().Err(err).Str("key", key).Msg("Failed to get tasks for stale worker")
						continue
					}

					// No tasks to recover
					if len(tasks) == 0 {
						log.Info().Str("key", key).Msg("Stale worker has no tasks to recover")
						r.client.Del(ctx, key)
						continue
					}

					// Return tasks to the main queue
					pipe := r.client.Pipeline()
					for taskID, taskData := range tasks {
						pipe.RPush(ctx, TaskQueueName, taskData)

						// Log recovery
						var msg TaskMessage
						if err := json.Unmarshal([]byte(taskData), &msg); err == nil {
							log.Info().
								Int64("execution_id", msg.ExecutionID).
								Str("worker", key).
								Str("task_id", taskID).
								Msg("Recovered stale task")
						} else {
							log.Error().
								Err(err).
								Str("worker", key).
								Str("task_id", taskID).
								Msg("Failed to unmarshal recovered task")
						}
					}

					// Delete the processing key
					pipe.Del(ctx, key)
					if _, err := pipe.Exec(ctx); err != nil {
						log.Error().Err(err).Str("key", key).Msg("Failed to recover tasks from stale worker")
					} else {
						log.Info().
							Str("key", key).
							Int("task_count", len(tasks)).
							Msg("Successfully recovered tasks from stale worker")
					}
				}
			}

		}
	}
}

// Close terminates the Redis connection
func (r *RedisClient) Close() error {
	return r.client.Close()
}
