package queue

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// TaskMessage represents a message sent to the queue
type TaskMessage struct {
	RunID       int64     `json:"run_id"`
	TaskID      int64     `json:"task_id"`
	Command     string    `json:"command"`
	ImageName   string    `json:"image_name,omitempty"`
	Timeout     int64     `json:"timeout_seconds"`
	MaxRetries  int       `json:"max_retries"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

// FormCommand breaks the single command line argument to its name and various args
func (t *TaskMessage) FormCommand() (string, []string, error) {
	var args []string
	var currentArg []rune
	var quoteChar rune
	inQuotes := false

	command := strings.TrimSpace(t.Command)
	nRunes := len(command)

	for i := 0; i < nRunes; {
		r := rune(command[i])
		switch {
		case inQuotes && r == '\\' && (i+1 < nRunes) && rune(command[i+1]) == quoteChar:
			// This is a very specific condition where the user is trying to escape the quoteChar
			// inside a quoted-arg
			i++
			currentArg = append(currentArg, rune(command[i]))

		case r == '"' || r == '\'':
			if !inQuotes {
				// Wasn't in a quoted command, but just entered one
				inQuotes = true
				quoteChar = r

				// if there was a previous command, we add it in now. Even if the argument is empty, because
				// we assume if someone used quotes, they know what they are doing with empty args
				if value := strings.TrimSpace(string(currentArg)); value != "" {
					args = append(args, value)
				}

				currentArg = []rune{}

			} else {
				// Is in a quoted command
				if r != quoteChar {
					// In this case, the quote could be double-quoted, but we see a single quote, or we are
					// in a single quote but saw a double quote. This means, that it's just part of the string
					currentArg = append(currentArg, r)
				} else {
					// otherwise, it's the close of the quote
					inQuotes = false

					// edge case where the next character is not a space, so we are joining the quote with a
					// non-quote. Basically, what this does is that if at the end of the quote, there is a space,
					// we add currentArg to args. Otherwise, we will continue building currentArgs
					if i+1 < nRunes {
						if nextRune := rune(command[i+1]); nextRune == ' ' || nextRune == '\t' {
							args = append(args, string(currentArg))
							currentArg = []rune{}
						}
					}
				}
			}
		case r == ' ' || r == '\t':
			if inQuotes {
				// We see a space-like character, and we are in quotes, we just save it
				currentArg = append(currentArg, r)
			} else if len(currentArg) > 0 {
				// Space like character but not in quote, this signifies the end of the command/arg
				// only add value if it is non-empty
				if value := strings.TrimSpace(string(currentArg)); len(value) > 0 {
					args = append(args, value)
				}
				currentArg = []rune{}
			}
		default:
			currentArg = append(currentArg, r)
		}
		i++
	}

	if len(currentArg) > 0 {
		if value := strings.TrimSpace(string(currentArg)); len(value) > 0 {
			args = append(args, value)
		}
	}

	if len(args) == 0 {
		return "", args, fmt.Errorf("'%s' is not a valid command", t.Command)
	}

	name := args[0]
	args = args[1:]

	return name, args, nil
}

// Client defines the interface for task queue operations
type Client interface {
	// Publish sends a task message to the queue
	Publish(ctx context.Context, message TaskMessage) error
	// Subscribe starts listening for messages and processes them with the handler. One client can only be subscribed once
	Subscribe(ctx context.Context, handler func(TaskMessage)) error
	// Close terminates the Client connection
	Close() error
}
