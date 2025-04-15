package loadtester

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
)

// TaskTypeGitHub represents the GitHub workflow type as string
const TaskTypeGitHub = "github"

// Environment variable names
const (
	ENV_GITHUB_REPO_OWNER    = "GITHUB_REPO_OWNER"
	ENV_GITHUB_REPO_NAME     = "GITHUB_REPO_NAME"
	ENV_GITHUB_TOKEN         = "GITHUB_TOKEN"
	ENV_GITHUB_POLL_INTERVAL = "GITHUB_POLL_INTERVAL"
	ENV_GITHUB_POLL_TIMEOUT  = "GITHUB_POLL_TIMEOUT"
	ENV_GITHUB_MAX_RETRIES   = "GITHUB_MAX_RETRIES"
)

// Default process values
const (
	DEFAULT_POLL_INTERVAL = 10
	DEFAULT_POLL_TIMEOUT  = 1800
	DEFAULT_MAX_RETRIES   = 12
)

// GitHub workflow statuses
const (
	statusCompleted = "completed"
	statusSuccess   = "success"
)

// GitHubTask represents a GitHub workflow dispatch task
type GitHubTask struct {
	TaskBase
	RepoOwner     string
	RepoName      string
	Token         string
	ClientPayload map[string]interface{}
	PollInterval  time.Duration
	PollTimeout   time.Duration
	MaxRetries    int
	httpClient    *http.Client
	workflowRunID string
}

// NewGitHubTask instantiates a new GitHub Task
func NewGitHubTask(metadata map[string]string, canary string, logger *zap.SugaredLogger) (*GitHubTask, error) {
	var pollIntervalInt, pollTimeoutInt int
	var err error

	repoOwner, foundOwner := metadata["repo_owner"]
	if !foundOwner {
		repoOwner, foundOwner = os.LookupEnv(ENV_GITHUB_REPO_OWNER)
		if !foundOwner {
			return nil, fmt.Errorf("`repo_owner` is required with type %s or set via %s env var", TaskTypeGitHub, ENV_GITHUB_REPO_OWNER)
		}
	}

	repoName, foundName := metadata["repo_name"]
	if !foundName {
		repoName, foundName = os.LookupEnv(ENV_GITHUB_REPO_NAME)
		if !foundName {
			return nil, fmt.Errorf("`repo_name` is required with type %s or set via %s env var", TaskTypeGitHub, ENV_GITHUB_REPO_NAME)
		}
	}

	token, foundToken := metadata["token"]
	if !foundToken {
		token, foundToken = os.LookupEnv(ENV_GITHUB_TOKEN)
		if !foundToken {
			return nil, fmt.Errorf("`token` is required with type %s or set via %s env var", TaskTypeGitHub, ENV_GITHUB_TOKEN)
		}
	}

	pollIntervalInt = DEFAULT_POLL_INTERVAL
	if val, found := metadata["poll_interval"]; found {
		pollIntervalInt, err = parseInt(val)
		if err != nil {
			return nil, fmt.Errorf("unable to convert `poll_interval` to int: %w", err)
		}
	} else if envVal, found := os.LookupEnv(ENV_GITHUB_POLL_INTERVAL); found {
		pollIntervalInt, err = parseInt(envVal)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %s env var to int: %w", ENV_GITHUB_POLL_INTERVAL, err)
		}
	}

	pollTimeoutInt = DEFAULT_POLL_TIMEOUT
	if val, found := metadata["poll_timeout"]; found {
		pollTimeoutInt, err = parseInt(val)
		if err != nil {
			return nil, fmt.Errorf("unable to convert `poll_timeout` to int: %w", err)
		}
	} else if envVal, found := os.LookupEnv(ENV_GITHUB_POLL_TIMEOUT); found {
		pollTimeoutInt, err = parseInt(envVal)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %s env var to int: %w", ENV_GITHUB_POLL_TIMEOUT, err)
		}
	}

	maxRetries := DEFAULT_MAX_RETRIES
	if val, found := metadata["max_retries"]; found {
		maxRetries, err = parseInt(val)
		if err != nil {
			return nil, fmt.Errorf("unable to convert `max_retries` to int: %w", err)
		}
	} else if envVal, found := os.LookupEnv(ENV_GITHUB_MAX_RETRIES); found {
		maxRetries, err = parseInt(envVal)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %s env var to int: %w", ENV_GITHUB_MAX_RETRIES, err)
		}
	}

	// Extract client payload from metadata
	// All keys with prefix "client_payload." will be added to the client payload
	clientPayload := make(map[string]interface{})
	for key, value := range metadata {
		if strings.HasPrefix(key, "client_payload.") {
			payloadKey := key[len("client_payload."):]
			clientPayload[payloadKey] = value
		}
	}

	clientPayload["canary"] = canary

	return &GitHubTask{
		TaskBase: TaskBase{
			canary: canary,
			logger: logger,
		},
		RepoOwner:     repoOwner,
		RepoName:      repoName,
		Token:         token,
		ClientPayload: clientPayload,
		PollInterval:  time.Duration(pollIntervalInt) * time.Second,
		PollTimeout:   time.Duration(pollTimeoutInt) * time.Second,
		MaxRetries:    maxRetries,
		httpClient:    &http.Client{Timeout: 60 * time.Second},
	}, nil
}

func parseInt(s string) (int, error) {
	var v int
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}

func (task *GitHubTask) Hash() string {
	// Create a unique hash by combining canary, repo info and client payload
	clientPayloadStr, _ := json.Marshal(task.ClientPayload)
	return hash(task.canary + task.RepoOwner + task.RepoName + string(clientPayloadStr))
}

func (task *GitHubTask) String() string {
	return fmt.Sprintf("GitHub workflow in %s/%s", task.RepoOwner, task.RepoName)
}

func (task *GitHubTask) Run(ctx context.Context) (*TaskRunResult, error) {
	eventID, err := task.dispatchWorkflow(ctx)
	if err != nil {
		task.logger.Errorf("failed to dispatch workflow: %s", err.Error())
		return &TaskRunResult{false, nil}, err
	}

	task.logger.Infof("dispatched workflow with event ID: %s", eventID)

	// Wait for workflow to start
	time.Sleep(10 * time.Second)

	// Find the workflow run ID
	err = task.findWorkflowRunID(ctx, eventID)
	if err != nil {
		task.logger.Errorf("failed to find workflow run ID: %s", err.Error())
		return &TaskRunResult{false, nil}, err
	}

	// Poll workflow status until completion
	ok, err := task.pollWorkflowStatus(ctx)
	if err != nil {
		task.logger.Errorf("workflow polling failed: %s", err.Error())
		return &TaskRunResult{false, nil}, err
	}
	return &TaskRunResult{ok, nil}, nil
}

// dispatchWorkflow triggers a GitHub workflow via repository_dispatch event
func (task *GitHubTask) dispatchWorkflow(ctx context.Context) (string, error) {
	// Generate a unique event type with timestamp and random hex
	randomBytes := make([]byte, 4)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	timestamp := time.Now().Format("20060102150405")
	randomHex := hex.EncodeToString(randomBytes)
	eventType := fmt.Sprintf("flagger-test-%s-%s", timestamp, randomHex)

	// Create payload for repository_dispatch
	type DispatchRequest struct {
		EventType     string                 `json:"event_type"`
		ClientPayload map[string]interface{} `json:"client_payload"`
	}

	payload := DispatchRequest{
		EventType:     eventType,
		ClientPayload: task.ClientPayload,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/dispatches", task.RepoOwner, task.RepoName)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "token "+task.Token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := task.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to dispatch workflow: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to dispatch workflow, status code: %d, body: %s", resp.StatusCode, string(body))
	}

	return eventType, nil
}

// findWorkflowRunID finds the workflow run ID by querying recent runs and matching the event type
func (task *GitHubTask) findWorkflowRunID(ctx context.Context, eventType string) error {
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/actions/runs?event=repository_dispatch&per_page=30", task.RepoOwner, task.RepoName)

	for attempt := 1; attempt <= task.MaxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Authorization", "token "+task.Token)
		req.Header.Set("Accept", "application/vnd.github+json")

		resp, err := task.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to get workflow runs: %w", err)
		}

		var data struct {
			WorkflowRuns []struct {
				ID           int64  `json:"id"`
				DisplayTitle string `json:"display_title"`
				Status       string `json:"status"`
			} `json:"workflow_runs"`
		}

		err = json.NewDecoder(resp.Body).Decode(&data)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}

		// Find the workflow run with display_title containing our event type
		for _, run := range data.WorkflowRuns {
			if run.DisplayTitle == eventType {
				task.workflowRunID = fmt.Sprintf("%d", run.ID)
				task.logger.Infof("found workflow run ID: %s", task.workflowRunID)
				return nil
			}
		}

		task.logger.Infof("attempt %d: workflow run not found yet, waiting...", attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(task.PollInterval):
			// Continue to next attempt
		}
	}

	return fmt.Errorf("failed to find workflow run ID after %d attempts", task.MaxRetries)
}

// pollWorkflowStatus polls the workflow status until completion
func (task *GitHubTask) pollWorkflowStatus(ctx context.Context) (bool, error) {
	ticker := time.NewTicker(task.PollInterval)
	defer ticker.Stop()

	timeout := time.After(task.PollTimeout)

	for {
		select {
		case <-ticker.C:
			url := fmt.Sprintf("https://api.github.com/repos/%s/%s/actions/runs/%s", task.RepoOwner, task.RepoName, task.workflowRunID)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return false, fmt.Errorf("failed to create request: %w", err)
			}

			req.Header.Set("Authorization", "token "+task.Token)
			req.Header.Set("Accept", "application/vnd.github+json")

			resp, err := task.httpClient.Do(req)
			if err != nil {
				return false, fmt.Errorf("failed to get workflow status: %w", err)
			}

			var data struct {
				Status     string `json:"status"`
				Conclusion string `json:"conclusion"`
			}

			err = json.NewDecoder(resp.Body).Decode(&data)
			resp.Body.Close()
			if err != nil {
				return false, fmt.Errorf("failed to decode response: %w", err)
			}

			task.logger.Infof("workflow status: %s, conclusion: %s", data.Status, data.Conclusion)

			if data.Status == statusCompleted {
				if data.Conclusion == statusSuccess {
					return true, nil
				}
				return false, fmt.Errorf("workflow failed with conclusion: %s", data.Conclusion)
			}

		case <-timeout:
			return false, fmt.Errorf("polling timed out after %s", task.PollTimeout)
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}
