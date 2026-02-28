package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
	_ "modernc.org/sqlite"
)

// --- API Response Types ---

type PipelineItem struct {
	ID          string `json:"id"`
	ProjectSlug string `json:"project_slug"`
	Number      int    `json:"number"`
	State       string `json:"state"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	Trigger     struct {
		Type       string `json:"type"`
		ReceivedAt string `json:"received_at"`
		Actor      struct {
			Login     string `json:"login"`
			AvatarURL string `json:"avatar_url"`
		} `json:"actor"`
	} `json:"trigger"`
	Vcs struct {
		ProviderName        string `json:"provider_name"`
		TargetRepositoryURL string `json:"target_repository_url"`
		OriginRepositoryURL string `json:"origin_repository_url"`
		Revision            string `json:"revision"`
		Branch              string `json:"branch"`
		Tag                 string `json:"tag"`
		ReviewID            string `json:"review_id"`
		ReviewURL           string `json:"review_url"`
		Commit              struct {
			Subject string `json:"subject"`
			Body    string `json:"body"`
		} `json:"commit"`
	} `json:"vcs"`
}

type PipelineResponse struct {
	Items         []PipelineItem `json:"items"`
	NextPageToken string         `json:"next_page_token"`
}

type WorkflowItem struct {
	ID              string `json:"id"`
	PipelineID      string `json:"pipeline_id"`
	Name            string `json:"name"`
	Status          string `json:"status"`
	CreatedAt       string `json:"created_at"`
	StoppedAt       string `json:"stopped_at"`
	PipelineNumber  int    `json:"pipeline_number"`
	ProjectSlug     string `json:"project_slug"`
	StartedBy       string `json:"started_by"`
	CanceledBy      string `json:"canceled_by"`
	ErroredBy       string `json:"errored_by"`
	Tag             string `json:"tag"`
	AutoRerunNumber int    `json:"auto_rerun_number"`
	MaxAutoReruns   int    `json:"max_auto_reruns"`
}

type PipelineWorkflowResponse struct {
	Items         []WorkflowItem `json:"items"`
	NextPageToken string         `json:"next_page_token"`
}

type WorkflowJobItem struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	Type              string `json:"type"`
	Status            string `json:"status"`
	JobNumber         int    `json:"job_number"`
	StartedAt         string `json:"started_at"`
	StoppedAt         string `json:"stopped_at"`
	ProjectSlug       string `json:"project_slug"`
	CanceledBy        string `json:"canceled_by"`
	ApprovedBy        string `json:"approved_by"`
	ApprovalRequestID string `json:"approval_request_id"`
}

type WorkflowJobsResponse struct {
	Items         []WorkflowJobItem `json:"items"`
	NextPageToken string            `json:"next_page_token"`
}

type JobResponse struct {
	CreatedAt   string `json:"created_at"`
	QueuedAt    string `json:"queued_at"`
	StartedAt   string `json:"started_at"`
	StoppedAt   string `json:"stopped_at"`
	Duration    int    `json:"duration"`
	Name        string `json:"name"`
	Number      int    `json:"number"`
	WebURL      string `json:"web_url"`
	Parallelism int    `json:"parallelism"`
	Status      string `json:"status"`
	Project     struct {
		ID          string `json:"id"`
		Slug        string `json:"slug"`
		Name        string `json:"name"`
		ExternalURL string `json:"external_url"`
	} `json:"project"`
	Executor struct {
		ResourceClass string `json:"resource_class"`
		Type          string `json:"type"`
	} `json:"executor"`
	Organization struct {
		Name string `json:"name"`
	} `json:"organization"`
	LatestWorkflow struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"latest_workflow"`
	Pipeline struct {
		ID string `json:"id"`
	} `json:"pipeline"`
}

type ProjectResponse struct {
	ID               string `json:"id"`
	Slug             string `json:"slug"`
	Name             string `json:"name"`
	OrganizationName string `json:"organization_name"`
	OrganizationSlug string `json:"organization_slug"`
	OrganizationID   string `json:"organization_id"`
	VcsInfo          struct {
		VcsURL        string `json:"vcs_url"`
		Provider      string `json:"provider"`
		DefaultBranch string `json:"default_branch"`
	} `json:"vcs_info"`
}

type OrgSummaryResponse struct {
	AllProjects []string `json:"all_projects"`
}

type WorkflowResponse struct {
	Items []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"items"`
	NextPageToken string `json:"next_page_token"`
}

type JobQueueInfo struct {
	Repository            string    `json:"repository"`
	JobName               string    `json:"job_name"`
	JobNumber             int       `json:"job_number"`
	JobID                 string    `json:"job_id"`
	Type                  string    `json:"type"`
	Status                string    `json:"status"`
	CreatedAt             time.Time `json:"created_at"`
	QueuedAt              time.Time `json:"queued_at"`
	StartedAt             time.Time `json:"started_at"`
	StoppedAt             time.Time `json:"stopped_at"`
	Duration              int       `json:"duration"`
	QueueTime             int64     `json:"queue_time"`
	WorkflowName          string    `json:"workflow_name"`
	WorkflowID            string    `json:"workflow_id"`
	PipelineID            string    `json:"pipeline_id"`
	ProjectSlug           string    `json:"project_slug"`
	CanceledBy            string    `json:"canceled_by"`
	ApprovedBy            string    `json:"approved_by"`
	ApprovalRequestID     string    `json:"approval_request_id"`
	WebURL                string    `json:"web_url"`
	Parallelism           int       `json:"parallelism"`
	ExecutorResourceClass string    `json:"executor_resource_class"`
	ExecutorType          string    `json:"executor_type"`
	OrganizationName      string    `json:"organization_name"`
	ProjectID             string    `json:"project_id"`
	ProjectName           string    `json:"project_name"`
	ProjectExternalURL    string    `json:"project_external_url"`
	LatestWorkflowID      string    `json:"latest_workflow_id"`
	LatestWorkflowName    string    `json:"latest_workflow_name"`
}

// --- CircleCI Client ---

type CircleCIClient struct {
	Token   string
	Client  *http.Client
	BaseURL string // default: https://circleci.com
}

func (c *CircleCIClient) baseURL() string {
	if c.BaseURL != "" {
		return c.BaseURL
	}
	return "https://circleci.com"
}

const (
	maxRetries     = 5
	initialBackoff = 1 * time.Second
)

func (c *CircleCIClient) doWithRetry(req *http.Request) (*http.Response, error) {
	for attempt := 0; attempt <= maxRetries; attempt++ {
		resp, err := c.Client.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
			return resp, nil
		}

		if attempt == maxRetries {
			return resp, nil
		}

		var wait time.Duration
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil {
				wait = time.Duration(seconds) * time.Second
			}
		}
		if wait == 0 {
			wait = initialBackoff * time.Duration(1<<uint(attempt))
		}
		jitter := time.Duration(float64(wait) * rand.Float64() * 0.5)
		wait += jitter

		resp.Body.Close()

		fmt.Fprintf(os.Stderr, "Rate limited (attempt %d/%d), waiting %v...\n", attempt+1, maxRetries, wait.Round(time.Millisecond))

		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(wait):
		}
	}

	return nil, fmt.Errorf("exceeded maximum retries")
}

func (c *CircleCIClient) GetJobDetails(ctx context.Context, projectSlug string, jobNumber int) (*JobResponse, error) {
	url := fmt.Sprintf("%s/api/v2/project/%s/job/%d", c.baseURL(), projectSlug, jobNumber)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Circle-Token", c.Token)
	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error for job %d: %s - %s (URL: %s)",
			jobNumber, resp.Status, string(body), url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body for job %d: %v", jobNumber, err)
	}

	var job JobResponse
	if err := json.Unmarshal(body, &job); err != nil {
		return nil, fmt.Errorf("JSON decode error for job %d: %v (body: %s)",
			jobNumber, err, string(body))
	}

	return &job, nil
}

func (c *CircleCIClient) GetWorkflows(ctx context.Context, projectSlug string) (*WorkflowResponse, error) {
	url := fmt.Sprintf("%s/api/v2/insights/%s/workflows/summary", c.baseURL(), projectSlug)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Circle-Token", c.Token)
	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var workflows WorkflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&workflows); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}

	return &workflows, nil
}

func (c *CircleCIClient) GetWorkflowJobs(ctx context.Context, workflowID string, pageToken string) (*WorkflowJobsResponse, error) {
	baseURL := fmt.Sprintf("%s/api/v2/workflow/%s/job", c.baseURL(), workflowID)
	url := baseURL
	if pageToken != "" {
		url = fmt.Sprintf("%s?page-token=%s", baseURL, pageToken)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Circle-Token", c.Token)
	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: %s - %s (URL: %s)", resp.Status, string(body), url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var jobs WorkflowJobsResponse
	if err := json.Unmarshal(body, &jobs); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v (body: %s)", err, string(body))
	}

	return &jobs, nil
}

func (c *CircleCIClient) GetPipelines(ctx context.Context, projectSlug string, pageToken string) (*PipelineResponse, error) {
	baseURL := fmt.Sprintf("%s/api/v2/project/%s/pipeline", c.baseURL(), projectSlug)
	url := baseURL
	if pageToken != "" {
		url = fmt.Sprintf("%s?page-token=%s", baseURL, pageToken)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Circle-Token", c.Token)
	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: %s - %s", resp.Status, string(body))
	}

	var pipelines PipelineResponse
	if err := json.NewDecoder(resp.Body).Decode(&pipelines); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}

	return &pipelines, nil
}

func (c *CircleCIClient) GetPipelineWorkflows(ctx context.Context, pipelineID string, pageToken string) (*PipelineWorkflowResponse, error) {
	baseURL := fmt.Sprintf("%s/api/v2/pipeline/%s/workflow", c.baseURL(), pipelineID)
	url := baseURL
	if pageToken != "" {
		url = fmt.Sprintf("%s?page-token=%s", baseURL, pageToken)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Circle-Token", c.Token)
	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: %s - %s", resp.Status, string(body))
	}

	var workflows PipelineWorkflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&workflows); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}

	return &workflows, nil
}

func (c *CircleCIClient) GetProject(ctx context.Context, projectSlug string) (*ProjectResponse, error) {
	url := fmt.Sprintf("%s/api/v2/project/%s", c.baseURL(), projectSlug)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Circle-Token", c.Token)
	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: %s - %s", resp.Status, string(body))
	}

	var project ProjectResponse
	if err := json.NewDecoder(resp.Body).Decode(&project); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}

	return &project, nil
}

func (c *CircleCIClient) GetOrgProjects(ctx context.Context, orgSlug string) ([]string, error) {
	url := fmt.Sprintf("%s/api/v2/insights/%s/summary", c.baseURL(), orgSlug)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Circle-Token", c.Token)
	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: %s - %s", resp.Status, string(body))
	}

	var summary OrgSummaryResponse
	if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}

	return summary.AllProjects, nil
}

// --- SQLite Writer ---

type SQLiteWriter struct {
	db *sql.DB
	mu sync.Mutex
}

func NewSQLiteWriter(dbPath string) (*SQLiteWriter, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database %s: %w", dbPath, err)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set WAL mode: %w", err)
	}

	w := &SQLiteWriter{db: db}
	if err := w.createTables(); err != nil {
		db.Close()
		return nil, err
	}
	return w, nil
}

func (w *SQLiteWriter) createTables() error {
	_, err := w.db.Exec(`
		CREATE TABLE IF NOT EXISTS projects (
			id TEXT PRIMARY KEY,
			slug TEXT,
			name TEXT,
			organization_name TEXT,
			organization_slug TEXT,
			organization_id TEXT,
			vcs_url TEXT,
			vcs_provider TEXT,
			vcs_default_branch TEXT
		);

		CREATE TABLE IF NOT EXISTS pipelines (
			id TEXT PRIMARY KEY,
			project_slug TEXT,
			number INTEGER,
			state TEXT,
			created_at TEXT,
			updated_at TEXT,
			trigger_type TEXT,
			trigger_received_at TEXT,
			trigger_actor_login TEXT,
			trigger_actor_avatar_url TEXT,
			vcs_provider_name TEXT,
			vcs_target_repository_url TEXT,
			vcs_origin_repository_url TEXT,
			vcs_revision TEXT,
			vcs_branch TEXT,
			vcs_tag TEXT,
			vcs_review_id TEXT,
			vcs_review_url TEXT,
			vcs_commit_subject TEXT,
			vcs_commit_body TEXT
		);

		CREATE TABLE IF NOT EXISTS workflows (
			id TEXT PRIMARY KEY,
			pipeline_id TEXT,
			name TEXT,
			status TEXT,
			created_at TEXT,
			stopped_at TEXT,
			pipeline_number INTEGER,
			project_slug TEXT,
			started_by TEXT,
			canceled_by TEXT,
			errored_by TEXT,
			tag TEXT,
			auto_rerun_number INTEGER,
			max_auto_reruns INTEGER
		);

		CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY,
			workflow_id TEXT,
			name TEXT,
			type TEXT,
			status TEXT,
			job_number INTEGER,
			project_slug TEXT,
			canceled_by TEXT,
			approved_by TEXT,
			approval_request_id TEXT,
			created_at TEXT,
			queued_at TEXT,
			started_at TEXT,
			stopped_at TEXT,
			duration INTEGER,
			web_url TEXT,
			parallelism INTEGER,
			executor_resource_class TEXT,
			executor_type TEXT,
			organization_name TEXT,
			project_id TEXT,
			project_name TEXT,
			project_external_url TEXT,
			pipeline_id TEXT,
			latest_workflow_id TEXT,
			latest_workflow_name TEXT,
			queue_time_ms INTEGER
		);
	`)
	return err
}

func (w *SQLiteWriter) InsertProject(p *ProjectResponse) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.db.Exec(`INSERT OR REPLACE INTO projects
		(id, slug, name, organization_name, organization_slug, organization_id,
		 vcs_url, vcs_provider, vcs_default_branch)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		p.ID, p.Slug, p.Name,
		p.OrganizationName, p.OrganizationSlug, p.OrganizationID,
		p.VcsInfo.VcsURL, p.VcsInfo.Provider, p.VcsInfo.DefaultBranch,
	)
	return err
}

func (w *SQLiteWriter) InsertPipeline(p PipelineItem) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.db.Exec(`INSERT OR REPLACE INTO pipelines
		(id, project_slug, number, state, created_at, updated_at,
		 trigger_type, trigger_received_at, trigger_actor_login, trigger_actor_avatar_url,
		 vcs_provider_name, vcs_target_repository_url, vcs_origin_repository_url,
		 vcs_revision, vcs_branch, vcs_tag, vcs_review_id, vcs_review_url,
		 vcs_commit_subject, vcs_commit_body)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		p.ID, p.ProjectSlug, p.Number, p.State, p.CreatedAt, p.UpdatedAt,
		p.Trigger.Type, p.Trigger.ReceivedAt, p.Trigger.Actor.Login, p.Trigger.Actor.AvatarURL,
		p.Vcs.ProviderName, p.Vcs.TargetRepositoryURL, p.Vcs.OriginRepositoryURL,
		p.Vcs.Revision, p.Vcs.Branch, p.Vcs.Tag, p.Vcs.ReviewID, p.Vcs.ReviewURL,
		p.Vcs.Commit.Subject, p.Vcs.Commit.Body,
	)
	return err
}

func (w *SQLiteWriter) InsertWorkflow(wf WorkflowItem) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.db.Exec(`INSERT OR REPLACE INTO workflows
		(id, pipeline_id, name, status, created_at, stopped_at,
		 pipeline_number, project_slug, started_by, canceled_by,
		 errored_by, tag, auto_rerun_number, max_auto_reruns)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		wf.ID, wf.PipelineID, wf.Name, wf.Status, wf.CreatedAt, wf.StoppedAt,
		wf.PipelineNumber, wf.ProjectSlug, wf.StartedBy, wf.CanceledBy,
		wf.ErroredBy, wf.Tag, wf.AutoRerunNumber, wf.MaxAutoReruns,
	)
	return err
}

func (w *SQLiteWriter) InsertJob(wj WorkflowJobItem, workflowID string, detail *JobResponse, queueTimeMs *int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if detail != nil {
		_, err := w.db.Exec(`INSERT OR REPLACE INTO jobs
			(id, workflow_id, name, type, status, job_number,
			 project_slug, canceled_by, approved_by, approval_request_id,
			 created_at, queued_at, started_at, stopped_at, duration,
			 web_url, parallelism,
			 executor_resource_class, executor_type,
			 organization_name,
			 project_id, project_name, project_external_url,
			 pipeline_id, latest_workflow_id, latest_workflow_name,
			 queue_time_ms)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			wj.ID, workflowID, wj.Name, wj.Type, wj.Status, nilIfZero(wj.JobNumber),
			wj.ProjectSlug, nilIfEmpty(wj.CanceledBy), nilIfEmpty(wj.ApprovedBy), nilIfEmpty(wj.ApprovalRequestID),
			detail.CreatedAt, detail.QueuedAt, detail.StartedAt, detail.StoppedAt, detail.Duration,
			detail.WebURL, detail.Parallelism,
			detail.Executor.ResourceClass, detail.Executor.Type,
			detail.Organization.Name,
			detail.Project.ID, detail.Project.Name, detail.Project.ExternalURL,
			detail.Pipeline.ID, detail.LatestWorkflow.ID, detail.LatestWorkflow.Name,
			queueTimeMs,
		)
		return err
	}

	// approval job — no detail available
	_, err := w.db.Exec(`INSERT OR REPLACE INTO jobs
		(id, workflow_id, name, type, status, job_number,
		 project_slug, canceled_by, approved_by, approval_request_id)
		VALUES (?,?,?,?,?,?,?,?,?,?)`,
		wj.ID, workflowID, wj.Name, wj.Type, wj.Status, nilIfZero(wj.JobNumber),
		wj.ProjectSlug, nilIfEmpty(wj.CanceledBy), nilIfEmpty(wj.ApprovedBy), nilIfEmpty(wj.ApprovalRequestID),
	)
	return err
}

func (w *SQLiteWriter) Close() error {
	return w.db.Close()
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nilIfZero(n int) any {
	if n == 0 {
		return nil
	}
	return n
}

// --- Project Slug Expansion ---

func expandProjectSlugs(ctx context.Context, client *CircleCIClient, projects []string) ([]string, error) {
	var expanded []string
	for _, p := range projects {
		if after, ok := strings.CutPrefix(p, "all:"); ok {
			orgSlug := after
			names, err := client.GetOrgProjects(ctx, orgSlug)
			if err != nil {
				return nil, fmt.Errorf("failed to get projects for org %s: %w", orgSlug, err)
			}
			for _, name := range names {
				expanded = append(expanded, orgSlug+"/"+name)
			}
		} else {
			expanded = append(expanded, p)
		}
	}
	return expanded, nil
}

// --- Process Project ---

type processProjectConfig struct {
	client       *CircleCIClient
	slug         string
	limit        int
	monthsSet    bool
	cutoff       time.Time
	silent       bool
	sqliteWriter *SQLiteWriter
	jobsChan     chan<- JobQueueInfo
}

func processProject(ctx context.Context, cfg processProjectConfig) error {
	slug := strings.Replace(cfg.slug, "github/", "gh/", 1)

	if cfg.sqliteWriter != nil {
		project, err := cfg.client.GetProject(ctx, slug)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to get project info for %s: %v\n", slug, err)
		} else {
			if err := cfg.sqliteWriter.InsertProject(project); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to insert project %s: %v\n", slug, err)
			}
		}
	}

	count := 0
	var nextPageToken string

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		pipelines, err := cfg.client.GetPipelines(ctx, slug, nextPageToken)
		if err != nil {
			return fmt.Errorf("error in %s: %v", slug, err)
		}

		tooOld := false
		for _, pipeline := range pipelines.Items {
			if !cfg.monthsSet && count >= cfg.limit {
				break
			}

			pipelineCreatedAt, err := time.Parse(time.RFC3339, pipeline.CreatedAt)
			if err == nil && pipelineCreatedAt.Before(cfg.cutoff) {
				tooOld = true
				break
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if cfg.sqliteWriter != nil {
				if err := cfg.sqliteWriter.InsertPipeline(pipeline); err != nil {
					fmt.Fprintf(os.Stderr, "Warning: failed to insert pipeline %s: %v\n", pipeline.ID, err)
				}
			}

			// Fetch workflows with pagination
			var allWorkflows []WorkflowItem
			var wfPageToken string
			for {
				workflows, err := cfg.client.GetPipelineWorkflows(ctx, pipeline.ID, wfPageToken)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error getting workflows for pipeline %s: %v\n", pipeline.ID, err)
					break
				}
				allWorkflows = append(allWorkflows, workflows.Items...)
				wfPageToken = workflows.NextPageToken
				if wfPageToken == "" {
					break
				}
			}

			hasProcessedPipeline := false

			for _, workflow := range allWorkflows {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				if cfg.sqliteWriter != nil {
					if err := cfg.sqliteWriter.InsertWorkflow(workflow); err != nil {
						fmt.Fprintf(os.Stderr, "Warning: failed to insert workflow %s: %v\n", workflow.ID, err)
					}
				}

				// Fetch jobs with pagination
				var allJobs []WorkflowJobItem
				var jobPageToken string
				for {
					jobs, err := cfg.client.GetWorkflowJobs(ctx, workflow.ID, jobPageToken)
					if err != nil {
						fmt.Fprintf(os.Stderr, "⚠️  Workflow %s (%s): %v\n", workflow.Name, workflow.ID, err)
						break
					}
					allJobs = append(allJobs, jobs.Items...)
					jobPageToken = jobs.NextPageToken
					if jobPageToken == "" {
						break
					}
				}

				for _, job := range allJobs {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					if job.JobNumber == 0 {
						if cfg.sqliteWriter != nil {
							if err := cfg.sqliteWriter.InsertJob(job, workflow.ID, nil, nil); err != nil {
								fmt.Fprintf(os.Stderr, "Warning: failed to insert approval job %s: %v\n", job.ID, err)
							}
						}
						if cfg.jobsChan != nil {
							info := JobQueueInfo{
								JobID:             job.ID,
								JobName:           job.Name,
								Type:              job.Type,
								Status:            job.Status,
								ProjectSlug:       job.ProjectSlug,
								CanceledBy:        job.CanceledBy,
								ApprovedBy:        job.ApprovedBy,
								ApprovalRequestID: job.ApprovalRequestID,
								WorkflowName:      workflow.Name,
								WorkflowID:        workflow.ID,
								PipelineID:        pipeline.ID,
							}
							select {
							case <-ctx.Done():
								return ctx.Err()
							case cfg.jobsChan <- info:
							}
						}
						hasProcessedPipeline = true
						continue
					}

					jobDetails, err := cfg.client.GetJobDetails(ctx, slug, job.JobNumber)
					if err != nil {
						fmt.Fprintf(os.Stderr, "⚠️  Job %d in workflow %s: %v\n", job.JobNumber, workflow.Name, err)
						continue
					}

					if cfg.sqliteWriter != nil {
						var queueTimeMs *int64
						createdAt, err1 := time.Parse(time.RFC3339, jobDetails.CreatedAt)
						startedAt, err2 := time.Parse(time.RFC3339, jobDetails.StartedAt)
						if err1 == nil && err2 == nil {
							ms := startedAt.Sub(createdAt).Milliseconds()
							queueTimeMs = &ms
						}
						if err := cfg.sqliteWriter.InsertJob(job, workflow.ID, jobDetails, queueTimeMs); err != nil {
							fmt.Fprintf(os.Stderr, "Warning: failed to insert job %s: %v\n", job.ID, err)
						}
						hasProcessedPipeline = true
					} else {
						createdAt, err := time.Parse(time.RFC3339, jobDetails.CreatedAt)
						if err != nil {
							continue
						}
						queuedAt, err := time.Parse(time.RFC3339, jobDetails.QueuedAt)
						if err != nil {
							continue
						}
						startedAt, err := time.Parse(time.RFC3339, jobDetails.StartedAt)
						if err != nil {
							continue
						}
						stoppedAt, _ := time.Parse(time.RFC3339, jobDetails.StoppedAt)

						info := JobQueueInfo{
							Repository:            jobDetails.Project.Slug,
							JobName:               jobDetails.Name,
							JobNumber:             jobDetails.Number,
							JobID:                 job.ID,
							Type:                  job.Type,
							Status:                job.Status,
							CreatedAt:             createdAt,
							QueuedAt:              queuedAt,
							StartedAt:             startedAt,
							StoppedAt:             stoppedAt,
							Duration:              jobDetails.Duration,
							QueueTime:             startedAt.Sub(createdAt).Milliseconds(),
							WorkflowName:          workflow.Name,
							WorkflowID:            workflow.ID,
							PipelineID:            pipeline.ID,
							ProjectSlug:           job.ProjectSlug,
							CanceledBy:            job.CanceledBy,
							ApprovedBy:            job.ApprovedBy,
							ApprovalRequestID:     job.ApprovalRequestID,
							WebURL:                jobDetails.WebURL,
							Parallelism:           jobDetails.Parallelism,
							ExecutorResourceClass: jobDetails.Executor.ResourceClass,
							ExecutorType:          jobDetails.Executor.Type,
							OrganizationName:      jobDetails.Organization.Name,
							ProjectID:             jobDetails.Project.ID,
							ProjectName:           jobDetails.Project.Name,
							ProjectExternalURL:    jobDetails.Project.ExternalURL,
							LatestWorkflowID:      jobDetails.LatestWorkflow.ID,
							LatestWorkflowName:    jobDetails.LatestWorkflow.Name,
						}

						select {
						case <-ctx.Done():
							return ctx.Err()
						case cfg.jobsChan <- info:
							hasProcessedPipeline = true
						}
					}
				}
			}

			if hasProcessedPipeline {
				count++
			}
		}

		if tooOld || (!cfg.monthsSet && count >= cfg.limit) {
			break
		}

		nextPageToken = pipelines.NextPageToken
		if nextPageToken == "" {
			break
		}
	}

	return nil
}

// --- Main ---

func newApp() *cli.App {
	return &cli.App{
		Name:  "circleci-queue-time",
		Usage: "Get queue times for CircleCI jobs",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:     "project",
				Aliases:  []string{"p"},
				Usage:    "Project slug (e.g. gh/org/repo) or all:{org-slug} for all projects in an org",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "token",
				Aliases: []string{"t"},
				Usage:   "CircleCI API token",
				EnvVars: []string{"CIRCLECI_TOKEN"},
			},
			&cli.StringFlag{
				Name:  "format",
				Value: "table",
				Usage: "Output format (table, ndjson, sqlite)",
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "Output file path (required for sqlite format)",
			},
			&cli.IntFlag{
				Name:  "limit",
				Value: 10,
				Usage: "Number of pipelines to fetch per project",
			},
			&cli.IntFlag{
				Name:  "months",
				Value: 1,
				Usage: "Number of months to look back",
			},
			&cli.BoolFlag{
				Name:  "silent",
				Usage: "Suppress all output except errors",
			},
		},
		Action: func(c *cli.Context) error {
			format := c.String("format")
			switch format {
			case "table", "ndjson", "sqlite":
			default:
				return fmt.Errorf("invalid format %q: must be table, ndjson, or sqlite", format)
			}
			if format == "sqlite" && c.String("output") == "" {
				return fmt.Errorf("--output flag is required when using sqlite format")
			}

			limit := c.Int("limit")
			monthsSet := c.IsSet("months")
			cutoff := time.Now().AddDate(0, -c.Int("months"), 0)

			client := &CircleCIClient{
				Token:  c.String("token"),
				Client: &http.Client{},
			}

			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
			defer stop()

			projects, err := expandProjectSlugs(ctx, client, c.StringSlice("project"))
			if err != nil {
				return err
			}

			var sqliteWriter *SQLiteWriter
			if format == "sqlite" {
				sw, err := NewSQLiteWriter(c.String("output"))
				if err != nil {
					return err
				}
				defer sw.Close()
				sqliteWriter = sw
			}

			var jobsChan chan JobQueueInfo
			var wg sync.WaitGroup
			if format != "sqlite" {
				jobsChan = make(chan JobQueueInfo)
				wg.Go(func() {
					if c.Bool("silent") {
						for range jobsChan {
						}
						return
					}
					if format == "ndjson" {
						for job := range jobsChan {
							json.NewEncoder(os.Stdout).Encode(job)
						}
					} else {
						fmt.Println("Repository\tWorkflow\tWorkflow ID\tPipeline ID\tJob\tJob ID\tNumber\tType\tStatus\tCreated At\tQueued At\tStarted At\tStopped At\tDuration\tQueue Time\tProject Slug\tCanceled By\tApproved By\tApproval Request ID\tWeb URL\tParallelism\tExecutor Resource Class\tExecutor Type\tOrganization\tProject ID\tProject Name\tProject External URL\tLatest Workflow ID\tLatest Workflow Name")
						fmt.Println("---------\t--------\t-----------\t-----------\t---\t------\t------\t----\t------\t----------\t---------\t----------\t----------\t--------\t----------\t------------\t-----------\t-----------\t-------------------\t-------\t-----------\t----------------------\t-------------\t------------\t----------\t------------\t--------------------\t------------------\t--------------------")
						for job := range jobsChan {
							fmt.Printf("%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
								job.Repository,
								job.WorkflowName,
								job.WorkflowID,
								job.PipelineID,
								job.JobName,
								job.JobID,
								job.JobNumber,
								job.Type,
								job.Status,
								job.CreatedAt.Format(time.RFC3339),
								job.QueuedAt.Format(time.RFC3339),
								job.StartedAt.Format(time.RFC3339),
								job.StoppedAt.Format(time.RFC3339),
								job.Duration,
								job.QueueTime,
								job.ProjectSlug,
								job.CanceledBy,
								job.ApprovedBy,
								job.ApprovalRequestID,
								job.WebURL,
								job.Parallelism,
								job.ExecutorResourceClass,
								job.ExecutorType,
								job.OrganizationName,
								job.ProjectID,
								job.ProjectName,
								job.ProjectExternalURL,
								job.LatestWorkflowID,
								job.LatestWorkflowName,
							)
						}
					}
				})
			}

			errChan := make(chan error, len(projects))
			var projectWg sync.WaitGroup
			projectWg.Add(len(projects))

			for _, projectSlug := range projects {
				go func(slug string) {
					defer projectWg.Done()
					if err := processProject(ctx, processProjectConfig{
						client:       client,
						slug:         slug,
						limit:        limit,
						monthsSet:    monthsSet,
						cutoff:       cutoff,
						silent:       c.Bool("silent"),
						sqliteWriter: sqliteWriter,
						jobsChan:     jobsChan,
					}); err != nil {
						errChan <- fmt.Errorf("❌ %v", err)
					}
				}(projectSlug)
			}

			projectWg.Wait()
			if jobsChan != nil {
				close(jobsChan)
			}

			close(errChan)
			var errors []error
			for err := range errChan {
				errors = append(errors, err)
			}

			wg.Wait()

			if len(errors) > 0 {
				fmt.Fprintln(os.Stderr, "\n🚫 Errors encountered:")
				for _, err := range errors {
					fmt.Fprintln(os.Stderr, err)
				}
				return fmt.Errorf("failed with %d error(s)", len(errors))
			}

			return nil
		},
	}
}

func main() {
	if err := newApp().Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
