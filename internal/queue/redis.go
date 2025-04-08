package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
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

// Subscribe starts listening for messages and processes them with the handler. One client can
// only be subscribed once
func (r *RedisClient) Subscribe(ctx context.Context, handler func(TaskMessage)) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			message, err := r.getNewMessage(ctx)
			if err != nil {
				log.Error().
					Err(err).
					Msg("Error encountered when fetching message from queue")
				continue
			}

			// Process message
			if err := processMessage(handler, *message); err != nil {
				log.Error().
					Err(err).
					Int64("run_id", message.RunID).
					Msg("Error encountered when processing message")
			}
		}
	}
}

func (r *RedisClient) getNewMessage(ctx context.Context) (*TaskMessage, error) {
	result, err := r.client.BLPop(ctx, 1*time.Second, TaskQueueName).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// No message available
			return nil, nil
		}
		return nil, fmt.Errorf("BLPOP from redis queue went bad. %w", err)
	}

	// Invalid message, this shouldn't usually happen
	if len(result) < 2 {
		return nil, nil
	}

	messageData := []byte(result[1])
	var message TaskMessage
	err = json.Unmarshal(messageData, &message)
	if err != nil {
		return nil, fmt.Errorf("could not parse message into TaskMessage. %w", err)
	}
	return &message, nil
}

func processMessage(handler func(TaskMessage), message TaskMessage) (err error) {
	defer func() {
		if rcv := recover(); rcv != nil {
			// Log the panic
			log.Error().Interface("panic", rcv).Int64("run_id", message.RunID).Msg("Handler panicked")

			err = fmt.Errorf("handler panicked: %v", rcv)
		}
	}()

	handler(message)
	return nil
}

// Close terminates the Redis connection
func (r *RedisClient) Close() error {
	return r.client.Close()
}
