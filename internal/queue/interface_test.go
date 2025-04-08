package queue_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"jobrunner/internal/queue"
)

func TestTaskMessage_FormCommand(t *testing.T) {
	tests := []struct {
		name           string
		command        string
		expectedName   string
		expectedArgs   []string
		expectError    bool
		errorSubstring string
	}{
		{
			name:         "simple command",
			command:      "echo hello",
			expectedName: "echo",
			expectedArgs: []string{"hello"},
			expectError:  false,
		},
		{
			name:         "command with multiple args",
			command:      "python my_script.py -m 1 -x debug",
			expectedName: "python",
			expectedArgs: []string{"my_script.py", "-m", "1", "-x", "debug"},
			expectError:  false,
		},
		{
			name:         "command with quoted args",
			command:      `python my_script.py -m "hello world" -x debug`,
			expectedName: "python",
			expectedArgs: []string{"my_script.py", "-m", "hello world", "-x", "debug"},
			expectError:  false,
		},
		{
			name:         "command with single quoted args",
			command:      `python my_script.py -m 'hello world' -x debug`,
			expectedName: "python",
			expectedArgs: []string{"my_script.py", "-m", "hello world", "-x", "debug"},
			expectError:  false,
		},
		{
			name:         "command with mixed quotes",
			command:      `python -c "print('hello world')" -x debug`,
			expectedName: "python",
			expectedArgs: []string{"-c", "print('hello world')", "-x", "debug"},
			expectError:  false,
		},
		{
			name:         "command with extra spaces",
			command:      "  echo   hello   world  ",
			expectedName: "echo",
			expectedArgs: []string{"hello", "world"},
			expectError:  false,
		},
		{
			name:         "command with quotes and extra spaces",
			command:      `  python   -m  "hello   world"  `,
			expectedName: "python",
			expectedArgs: []string{"-m", "hello   world"},
			expectError:  false,
		},
		{
			name:           "empty command",
			command:        "",
			expectedName:   "",
			expectedArgs:   []string{},
			expectError:    true,
			errorSubstring: "not a valid command",
		},
		{
			name:           "only spaces",
			command:        "   ",
			expectedName:   "",
			expectedArgs:   []string{},
			expectError:    true,
			errorSubstring: "not a valid command",
		},
		{
			name:         "command with quotes followed by non-space",
			command:      `echo "hello"world`,
			expectedName: "echo",
			expectedArgs: []string{"helloworld"},
			expectError:  false,
		},
		{
			name:         "complex command with multiple quotes",
			command:      `find . -name "*.go" -exec grep -l "fmt.Println" {} \;`,
			expectedName: "find",
			expectedArgs: []string{".", "-name", "*.go", "-exec", "grep", "-l", "fmt.Println", "{}", "\\;"},
			expectError:  false,
		},
		{
			name:         "command with escaped quotes",
			command:      `echo "hello \"quoted\" world"`,
			expectedName: "echo",
			expectedArgs: []string{`hello "quoted" world`},
			expectError:  false,
		},
		{
			name:         "docker command",
			command:      `docker run -it --rm -v "$(pwd):/app" ubuntu:latest bash -c "echo hello"`,
			expectedName: "docker",
			expectedArgs: []string{"run", "-it", "--rm", "-v", "$(pwd):/app", "ubuntu:latest", "bash", "-c", "echo hello"},
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &queue.TaskMessage{
				RunID:       1,
				TaskID:      1,
				Command:     tt.command,
				Timeout:     60,
				MaxRetries:  3,
				ScheduledAt: time.Now(),
			}

			cmdName, args, err := msg.FormCommand()

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorSubstring != "" {
					assert.Contains(t, err.Error(), tt.errorSubstring)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedName, cmdName)
				assert.Equal(t, tt.expectedArgs, args)
			}
		})
	}
}

func TestTaskMessage_FormCommand_EdgeCases(t *testing.T) {
	t.Run("single word command", func(t *testing.T) {
		msg := &queue.TaskMessage{Command: "ls"}
		cmdName, args, err := msg.FormCommand()
		require.NoError(t, err)
		assert.Equal(t, "ls", cmdName)
		assert.Empty(t, args)
	})

	t.Run("command with unclosed quotes", func(t *testing.T) {
		// Note: The current implementation doesn't actually check for unclosed quotes,
		// it will just include everything up to the end as part of the quoted argument.
		// This test documents the current behavior rather than checking for an error.
		msg := &queue.TaskMessage{Command: `echo "hello world`}
		cmdName, args, err := msg.FormCommand()
		require.NoError(t, err)
		assert.Equal(t, "echo", cmdName)
		assert.Equal(t, []string{"hello world"}, args)
	})

	t.Run("empty quoted argument", func(t *testing.T) {
		msg := &queue.TaskMessage{Command: `echo "" empty`}
		cmdName, args, err := msg.FormCommand()
		require.NoError(t, err)
		assert.Equal(t, "echo", cmdName)
		assert.Equal(t, []string{"", "empty"}, args)
	})

	t.Run("only quoted argument", func(t *testing.T) {
		msg := &queue.TaskMessage{Command: `"only quoted"`}
		cmdName, args, err := msg.FormCommand()
		require.NoError(t, err)
		assert.Equal(t, "only quoted", cmdName)
		assert.Empty(t, args)
	})
}
