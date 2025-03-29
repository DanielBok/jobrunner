package queue

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	TaskQueueName = "jobrunner:tasks"
)

// RedisClient implements Client using Redis
type RedisClient struct {
	client *redis.Client
}

// NewRedisClient creates a new Redis queue client
func NewRedisClient(addr, password string, db int) (*RedisClient, error) {
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

	return &RedisClient{client: client}, nil
}

// Publish sends a task message to the queue
func (r *RedisClient) Publish(ctx context.Context, message TaskMessage) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return r.client.RPush(ctx, TaskQueueName, data).Err()
}

// Subscribe starts listening for messages and processes them with the handler
func (r *RedisClient) Subscribe(ctx context.Context, handler func(TaskMessage) error) error {
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

			var message TaskMessage
			if err := json.Unmarshal([]byte(result[1]), &message); err != nil {
				// Log error but continue processing
				continue
			}

			// Process message
			if err := handler(message); err != nil {
				// Log error but continue processing
				continue
			}
		}
	}
}

// Close terminates the Redis connection
func (r *RedisClient) Close() error {
	return r.client.Close()
}
