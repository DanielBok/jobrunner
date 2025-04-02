package queue_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jobrunner/internal/queue"
)

// test heartbeat interval
var heartbeatInterval int64 = 1

// testRedis provides connection details for the test Redis instance
var testRedis = struct {
	Addr     string
	Password string
	DB       int
}{
	Addr:     "localhost:6379",
	Password: "redis",
	DB:       1, // Use a different DB than the main app
}

// Helper to clean up Redis before/after tests
func cleanupRedis() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     testRedis.Addr,
		Password: testRedis.Password,
		DB:       testRedis.DB,
	})

	// Clear test keys
	ctx := context.Background()
	client.Del(ctx, queue.TaskQueueName)
	client.Del(ctx, queue.DeadLetterQueueName)

	// Clear any processing keys
	keys, err := client.Keys(ctx, "jobrunner:processing:*").Result()
	if err == nil && len(keys) > 0 {
		client.Del(ctx, keys...)
	}

	return client
}

func TestNewRedisClient(t *testing.T) {
	// Clean up before test
	err := cleanupRedis().Close()
	require.NoError(t, err)

	// Test successful connection
	t.Run("successful connection", func(t *testing.T) {
		client, err := queue.NewRedisClient(testRedis.Addr, testRedis.Password, testRedis.DB, heartbeatInterval)
		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer func() {
			err := client.Close()
			assert.NoError(t, err)
		}()
	})

	// Test connection failure
	t.Run("connection failure", func(t *testing.T) {
		client, err := queue.NewRedisClient("invalid:6379", "", 0, heartbeatInterval)
		assert.Error(t, err)
		assert.Nil(t, client)
	})
}

func TestRedisClient_Publish(t *testing.T) {
	// Clean up before test
	redisClient := cleanupRedis()
	defer func() {
		err := redisClient.Close()
		assert.NoError(t, err)
	}()

	// Create queue client
	client, err := queue.NewRedisClient(testRedis.Addr, testRedis.Password, testRedis.DB, heartbeatInterval)
	require.NoError(t, err)
	defer func() {
		err := client.Close()
		assert.NoError(t, err)
	}()

	ctx := context.Background()

	// Test publishing a message
	t.Run("publish message", func(t *testing.T) {
		// Create a message
		msg := queue.TaskMessage{
			ExecutionID: 1,
			JobID:       100,
			Command:     "echo test",
			Timeout:     60,
			MaxRetries:  3,
			ScheduledAt: time.Now(),
		}

		// Publish
		err := client.Publish(ctx, msg)
		assert.NoError(t, err)

		// Verify message was added to queue
		length, err := redisClient.LLen(ctx, queue.TaskQueueName).Result()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), length)

		// Verify message content
		result, err := redisClient.LPop(ctx, queue.TaskQueueName).Result()
		assert.NoError(t, err)

		var decodedMsg queue.TaskMessage
		err = json.Unmarshal([]byte(result), &decodedMsg)
		assert.NoError(t, err)
		assert.Equal(t, msg.ExecutionID, decodedMsg.ExecutionID)
		assert.Equal(t, msg.JobID, decodedMsg.JobID)
		assert.Equal(t, msg.Command, decodedMsg.Command)
	})

	// Test publishing with a cancelled context
	t.Run("publish with cancelled context", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		msg := queue.TaskMessage{
			ExecutionID: 2,
			JobID:       101,
			Command:     "echo test2",
		}

		err := client.Publish(cancelCtx, msg)
		assert.Error(t, err)
	})
}

func TestRedisClient_Subscribe(t *testing.T) {
	// Clean up before test
	redisClient := cleanupRedis()
	defer func() {
		err := redisClient.Close()
		assert.NoError(t, err)
	}()

	ctx := context.Background()

	t.Run("subscription processes messages", func(t *testing.T) {
		redisClient.Del(ctx, queue.TaskQueueName, queue.DeadLetterQueueName)

		// Create queue client
		client, err := queue.NewRedisClient(testRedis.Addr, testRedis.Password, testRedis.DB, heartbeatInterval)
		require.NoError(t, err)
		defer func() {
			err := client.Close()
			assert.NoError(t, err)
		}()

		// Create test messages
		msgs := []queue.TaskMessage{
			{
				ExecutionID: 1,
				JobID:       100,
				Command:     "echo test1",
				Timeout:     60,
				MaxRetries:  3,
				ScheduledAt: time.Now(),
			},
			{
				ExecutionID: 2,
				JobID:       101,
				Command:     "echo test2",
				Timeout:     120,
				MaxRetries:  2,
				ScheduledAt: time.Now(),
			},
		}

		// Setup to track processed messages
		var processedMsgs []queue.TaskMessage
		var mu sync.Mutex
		var wg sync.WaitGroup
		wg.Add(len(msgs)) // Expect 2 messages

		// Setup subscription context with timeout
		subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		// Start subscription in a goroutine
		go func() {
			handler := func(msg queue.TaskMessage) error {
				mu.Lock()
				processedMsgs = append(processedMsgs, msg)
				mu.Unlock()
				wg.Done()
				return nil
			}

			err := client.Subscribe(subCtx, handler)
			assert.Error(t, err) // Should error due to context timeout
		}()

		// Give subscription time to start
		time.Sleep(500 * time.Millisecond)

		// Add messages to queue (after subscription has started)
		for _, msg := range msgs {
			data, err := json.Marshal(msg)
			require.NoError(t, err)
			redisClient.RPush(ctx, queue.TaskQueueName, data)
		}

		// Wait for all messages to be processed with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success, continue
		case <-time.After(5 * time.Second):
			t.Fatal("Timed out waiting for messages to be processed")
		}

		// Check processed messages
		mu.Lock()
		defer mu.Unlock()
		assert.Len(t, processedMsgs, len(msgs))

		// Check if messages were processed in order
		assert.Equal(t, int64(1), processedMsgs[0].ExecutionID)
		assert.Equal(t, int64(2), processedMsgs[1].ExecutionID)
	})

	t.Run("handler error sends to dead letter queue", func(t *testing.T) {
		// Clear the dead letter queue
		redisClient.Del(ctx, queue.TaskQueueName, queue.DeadLetterQueueName)

		// Create queue client
		client, err := queue.NewRedisClient(testRedis.Addr, testRedis.Password, testRedis.DB, heartbeatInterval)
		require.NoError(t, err)
		defer func() {
			err := client.Close()
			assert.NoError(t, err)
		}()

		// Create a test message
		msg := queue.TaskMessage{
			ExecutionID: 3,
			JobID:       102,
			Command:     "echo test_error",
			Timeout:     30,
			MaxRetries:  1,
			ScheduledAt: time.Now(),
		}

		// Setup subscription context with timeout
		subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)

		// Start subscription in a goroutine
		go func() {
			handler := func(msg queue.TaskMessage) error {
				wg.Done()
				return fmt.Errorf("test error")
			}

			err := client.Subscribe(subCtx, handler)
			assert.Error(t, err) // Should error due to context timeout
		}()

		// Give subscription time to start
		time.Sleep(500 * time.Millisecond)

		// Add message to queue after subscription has started
		data, err := json.Marshal(msg)
		require.NoError(t, err)
		redisClient.RPush(ctx, queue.TaskQueueName, data)

		// Wait for message to be processed with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success, continue
		case <-time.After(5 * time.Second):
			t.Fatal("Timed out waiting for message to be processed")
		}

		// Give time for the DLQ operation to complete
		time.Sleep(500 * time.Millisecond)

		// Check dead letter queue
		dlqLength, err := redisClient.LLen(ctx, queue.DeadLetterQueueName).Result()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), dlqLength)

		// Verify dead letter message
		result, err := redisClient.LPop(ctx, queue.DeadLetterQueueName).Result()
		assert.NoError(t, err)

		var deadMsg map[string]interface{}
		err = json.Unmarshal([]byte(result), &deadMsg)
		assert.NoError(t, err)

		// Check message structure
		assert.Contains(t, deadMsg, "message")
		assert.Contains(t, deadMsg, "error")
		assert.Contains(t, deadMsg, "timestamp")
		assert.Contains(t, deadMsg, "worker_id")

		// Check error message
		assert.Equal(t, "test error", deadMsg["error"])

		// Verify original message
		msgData := deadMsg["message"].(map[string]interface{})
		assert.Equal(t, float64(3), msgData["execution_id"])
		assert.Equal(t, float64(102), msgData["job_id"])
	})

	t.Run("handler panic is recovered", func(t *testing.T) {
		redisClient.Del(ctx, queue.TaskQueueName, queue.DeadLetterQueueName)
		// Create queue client
		client, err := queue.NewRedisClient(testRedis.Addr, testRedis.Password, testRedis.DB, heartbeatInterval)
		require.NoError(t, err)
		defer func() {
			err := client.Close()
			assert.NoError(t, err)
		}()

		// Create a test message
		msg := queue.TaskMessage{
			ExecutionID: 4,
			JobID:       103,
			Command:     "echo test_panic",
			Timeout:     30,
			MaxRetries:  1,
			ScheduledAt: time.Now(),
		}

		// Setup subscription context with timeout
		subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)

		// Start subscription in a goroutine
		go func() {
			handler := func(msg queue.TaskMessage) error {
				wg.Done()
				panic("test panic")
			}

			err := client.Subscribe(subCtx, handler)
			assert.Error(t, err) // Should error due to context timeout
		}()

		// Give subscription time to start
		time.Sleep(500 * time.Millisecond)

		// Add message to queue after subscription has started
		data, err := json.Marshal(msg)
		require.NoError(t, err)
		redisClient.RPush(ctx, queue.TaskQueueName, data)

		// Wait for message to be processed with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success, continue
		case <-time.After(3 * time.Second):
			t.Fatal("Timed out waiting for message to be processed")
		}

		// Give time for the DLQ operation to complete
		time.Sleep(500 * time.Millisecond)

		// Check dead letter queue
		dlqLength, err := redisClient.LLen(ctx, queue.DeadLetterQueueName).Result()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), dlqLength)

		// Verify dead letter message
		result, err := redisClient.LPop(ctx, queue.DeadLetterQueueName).Result()
		assert.NoError(t, err)

		var deadMsg map[string]interface{}
		err = json.Unmarshal([]byte(result), &deadMsg)
		assert.NoError(t, err)

		// Check error contains panic info
		assert.Contains(t, deadMsg["error"].(string), "handler panicked")
	})

	t.Run("already subscribed client returns error", func(t *testing.T) {
		redisClient.Del(ctx, queue.TaskQueueName, queue.DeadLetterQueueName)

		// Create a new client and subscribe once
		client, err := queue.NewRedisClient(testRedis.Addr, testRedis.Password, testRedis.DB, heartbeatInterval)
		require.NoError(t, err)
		defer func() {
			err := client.Close()
			assert.NoError(t, err)
		}()

		// Create a cancelable context
		subCtx, cancel := context.WithCancel(ctx)

		var wg sync.WaitGroup
		wg.Add(1)

		// Subscribe in a goroutine
		go func() {
			defer wg.Done()
			handler := func(msg queue.TaskMessage) error {
				return nil
			}
			err := client.Subscribe(subCtx, handler)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "context canceled")
		}()

		// Allow time for subscription to start
		time.Sleep(100 * time.Millisecond)

		// Try to subscribe again with the same client
		err = client.Subscribe(ctx, func(msg queue.TaskMessage) error {
			return nil
		})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already subscribed")

		// Clean up
		time.Sleep(100 * time.Millisecond)
		cancel()
		wg.Wait()
	})
}

//func TestRedisClient_RecoverStaleTasks(t *testing.T) {
//	// Clean up before test
//	redisClient := cleanupRedis()
//	defer func() {
//		err := redisClient.Close()
//		assert.NoError(t, err)
//	}()
//
//	ctx := context.Background()
//	redisClient.Del(ctx, queue.TaskQueueName, queue.DeadLetterQueueName)
//
//	// Create a "processing" key
//	processingKey := "jobrunner:processing:test-worker"
//
//	// Create test message
//	msg := queue.TaskMessage{
//		ExecutionID: 5,
//		JobID:       104,
//		Command:     "echo test_recover",
//		Timeout:     30,
//		MaxRetries:  1,
//		ScheduledAt: time.Now(),
//	}
//
//	data, err := json.Marshal(msg)
//	require.NoError(t, err)
//
//	// Store message in processing hash with a very short TTL
//	// This forces the key to be considered stale during recovery
//	redisClient.HSet(ctx, processingKey, fmt.Sprintf("%d", msg.ExecutionID), data)
//
//	// Set TTL to 1 second (Redis minimum)
//	redisClient.Expire(ctx, processingKey, 1*time.Second)
//
//	// Verify key exists
//	exists, err := redisClient.Exists(ctx, processingKey).Result()
//	assert.NoError(t, err)
//	assert.Equal(t, int64(1), exists)
//
//	// Create queue client with shorter heartbeat for testing
//	// This ensures our task will be seen as stale
//	client, err := queue.NewRedisClient(testRedis.Addr, testRedis.Password, testRedis.DB, 5) // 5 second heartbeat
//	require.NoError(t, err)
//	defer func() {
//		err := client.Close()
//		assert.NoError(t, err)
//	}()
//
//	// Manually make TTL very low to trigger recovery
//	// This simulates the TTL being nearly expired
//	redisClient.PExpire(ctx, processingKey, 1*time.Millisecond) // 100ms TTL
//
//	// Wait briefly to let TTL get very low but not completely expire
//	time.Sleep(10 * time.Millisecond)
//
//	// Run recovery in a goroutine
//	recoverCtx, cancel := context.WithCancel(ctx)
//	defer cancel()
//
//	go func() {
//		client.RecoverStaleTasks(recoverCtx)
//	}()
//
//	// Wait for recovery to happen
//	time.Sleep(2 * time.Second)
//
//	// Check if task was recovered and moved back to queue
//	queueLength, err := redisClient.LLen(ctx, queue.TaskQueueName).Result()
//	assert.NoError(t, err)
//	assert.Equal(t, int64(1), queueLength)
//
//	// Check if processing key was deleted
//	exists, err = redisClient.Exists(ctx, processingKey).Result()
//	assert.NoError(t, err)
//	assert.Equal(t, int64(0), exists)
//
//	// Verify recovered message
//	result, err := redisClient.LPop(ctx, queue.TaskQueueName).Result()
//	assert.NoError(t, err)
//
//	var recoveredMsg queue.TaskMessage
//	err = json.Unmarshal([]byte(result), &recoveredMsg)
//	assert.NoError(t, err)
//	assert.Equal(t, msg.ExecutionID, recoveredMsg.ExecutionID)
//	assert.Equal(t, msg.JobID, recoveredMsg.JobID)
//}

func TestRedisClient_Close(t *testing.T) {
	// Clean up before test
	err := cleanupRedis().Close()
	require.NoError(t, err)

	// Create a new client
	client, err := queue.NewRedisClient(testRedis.Addr, testRedis.Password, testRedis.DB, heartbeatInterval)
	require.NoError(t, err)

	// Close should work without error
	err = client.Close()
	assert.NoError(t, err)

	// Attempting operations after close should fail
	ctx := context.Background()
	err = client.Publish(ctx, queue.TaskMessage{ExecutionID: 999})
	assert.Error(t, err)
}
