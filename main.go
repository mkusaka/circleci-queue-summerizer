package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/briandowns/spinner"
	"github.com/mattn/go-isatty"
	"github.com/urfave/cli/v2"
	_ "modernc.org/sqlite"
)

// --- Terminal Status Sets ---

var terminalWorkflowStatuses = map[string]bool{
	"success":      true,
	"failed":       true,
	"error":        true,
	"canceled":     true,
	"unauthorized": true,
}

var terminalJobStatuses = map[string]bool{
	"success":             true,
	"failed":              true,
	"error":               true,
	"canceled":            true,
	"infrastructure_fail": true,
	"timedout":            true,
	"not_run":             true,
}

var sincePattern = regexp.MustCompile(`^(\d+)\s*([a-zA-Z]+)$`)

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
	Warnf   func(format string, args ...any)
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
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	return doRequestWithRetry(req, client.Do, c.Warnf)
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

	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set busy_timeout: %w", err)
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

// IsPipelineFullyProcessed returns true if the pipeline has at least one workflow,
// all workflows are in a terminal status, and every workflow has at least one job.
func (w *SQLiteWriter) IsPipelineFullyProcessed(pipelineID string) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rows, err := w.db.Query(
		`SELECT w.status, (SELECT COUNT(*) FROM jobs j WHERE j.workflow_id = w.id) as job_count
		 FROM workflows w WHERE w.pipeline_id = ?`, pipelineID)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var status string
		var jobCount int
		if err := rows.Scan(&status, &jobCount); err != nil {
			return false, err
		}
		if !terminalWorkflowStatuses[status] || jobCount == 0 {
			return false, nil
		}
		count++
	}
	return count > 0, rows.Err()
}

// IsWorkflowFullyProcessed returns true if the workflow exists with a terminal status
// and has at least one job stored.
func (w *SQLiteWriter) IsWorkflowFullyProcessed(workflowID string) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var status string
	err := w.db.QueryRow(`SELECT status FROM workflows WHERE id = ?`, workflowID).Scan(&status)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !terminalWorkflowStatuses[status] {
		return false, nil
	}

	var jobCount int
	err = w.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE workflow_id = ?`, workflowID).Scan(&jobCount)
	if err != nil {
		return false, err
	}
	return jobCount > 0, nil
}

// IsJobComplete returns true if the job exists with a terminal status and has
// detail data (created_at IS NOT NULL, indicating GetJobDetails was called).
func (w *SQLiteWriter) IsJobComplete(jobID string) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var status string
	var createdAt sql.NullString
	err := w.db.QueryRow(`SELECT status, created_at FROM jobs WHERE id = ?`, jobID).Scan(&status, &createdAt)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return terminalJobStatuses[status] && createdAt.Valid, nil
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

func isJobDetailsNotFound(err error) bool {
	return isJobDetailsNotFoundError(err)
}

func shouldSkipJobDetails(job WorkflowJobItem) bool {
	if job.JobNumber == 0 || job.Status == "not_run" {
		return true
	}

	// lock/unlock jobs (serial-group control) appear in workflow jobs, but
	// project job-details endpoint often returns 404 for them.
	if job.Type == "lock" || job.Type == "unlock" {
		return true
	}

	// For canceled jobs, missing started_at strongly indicates the job never
	// actually ran and detail endpoint is typically unavailable.
	return job.Status == "canceled" && strings.TrimSpace(job.StartedAt) == ""
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

// --- Warning Buffer ---

// warningBuffer collects warning messages when the spinner is active,
// then flushes them after the spinner stops so they don't interleave.
type warningBuffer struct {
	mu       sync.Mutex
	warnings []string
	buffered bool // true = collect into buffer; false = write to stderr immediately
}

func (wb *warningBuffer) warnf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if wb.buffered {
		wb.mu.Lock()
		wb.warnings = append(wb.warnings, msg)
		wb.mu.Unlock()
	} else {
		fmt.Fprint(os.Stderr, msg)
	}
}

func (wb *warningBuffer) flush() {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if len(wb.warnings) > 0 {
		fmt.Fprintf(os.Stderr, "\n⚠️  %d warning(s) during processing:\n", len(wb.warnings))
		for _, w := range wb.warnings {
			fmt.Fprint(os.Stderr, w)
		}
	}
}

// --- Processing Stats ---

type processingStats struct {
	mu               sync.Mutex
	pipelinesTotal   int
	pipelinesSkipped int
	workflowsTotal   int
	workflowsSkipped int
	jobsTotal        int
	jobsSkipped      int
}

func (s *processingStats) incPipeline(skipped bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pipelinesTotal++
	if skipped {
		s.pipelinesSkipped++
	}
}

func (s *processingStats) incWorkflow(skipped bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workflowsTotal++
	if skipped {
		s.workflowsSkipped++
	}
}

func (s *processingStats) incJob(skipped bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobsTotal++
	if skipped {
		s.jobsSkipped++
	}
}

func (s *processingStats) summary() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("Processed %d pipelines (%d skipped), %d workflows (%d skipped), %d jobs (%d skipped)",
		s.pipelinesTotal, s.pipelinesSkipped,
		s.workflowsTotal, s.workflowsSkipped,
		s.jobsTotal, s.jobsSkipped)
}

func (s *processingStats) spinnerSuffix() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf(" %d pipelines, %d workflows, %d jobs processed...",
		s.pipelinesTotal, s.workflowsTotal, s.jobsTotal)
}

// --- Process Project ---

type processProjectConfig struct {
	client        *CircleCIClient
	slug          string
	limit         int
	timeFilterSet bool
	cutoff        time.Time
	verbose       bool
	sqliteWriter  *SQLiteWriter
	jobsChan      chan<- JobQueueInfo
	stats         *processingStats
	onProgress    func()
	warnf         func(format string, args ...any)
}

func processProject(ctx context.Context, cfg processProjectConfig) error {
	if cfg.warnf == nil {
		cfg.warnf = func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, format, args...)
		}
	}

	slug := strings.Replace(cfg.slug, "github/", "gh/", 1)

	if cfg.sqliteWriter != nil {
		project, err := cfg.client.GetProject(ctx, slug)
		if err != nil {
			cfg.warnf("Warning: failed to get project info for %s: %v\n", slug, err)
		} else {
			if err := cfg.sqliteWriter.InsertProject(project); err != nil {
				cfg.warnf("Warning: failed to insert project %s: %v\n", slug, err)
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
			if !cfg.timeFilterSet && count >= cfg.limit {
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
					cfg.warnf("Warning: failed to insert pipeline %s: %v\n", pipeline.ID, err)
				}

				fullyProcessed, lookupErr := cfg.sqliteWriter.IsPipelineFullyProcessed(pipeline.ID)
				if lookupErr != nil {
					cfg.warnf("Warning: DB lookup failed for pipeline %s: %v\n", pipeline.ID, lookupErr)
				} else if fullyProcessed {
					if cfg.verbose {
						fmt.Fprintf(os.Stderr, "Skipping pipeline %s (fully processed in DB)\n", pipeline.ID)
					}
					if cfg.stats != nil {
						cfg.stats.incPipeline(true)
						if cfg.onProgress != nil {
							cfg.onProgress()
						}
					}
					count++
					continue
				}
			}

			// Fetch workflows with pagination
			var allWorkflows []WorkflowItem
			var wfPageToken string
			for {
				workflows, err := cfg.client.GetPipelineWorkflows(ctx, pipeline.ID, wfPageToken)
				if err != nil {
					cfg.warnf("Error getting workflows for pipeline %s: %v\n", pipeline.ID, err)
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
						cfg.warnf("Warning: failed to insert workflow %s: %v\n", workflow.ID, err)
					}

					wfProcessed, lookupErr := cfg.sqliteWriter.IsWorkflowFullyProcessed(workflow.ID)
					if lookupErr != nil {
						cfg.warnf("Warning: DB lookup failed for workflow %s: %v\n", workflow.ID, lookupErr)
					} else if wfProcessed {
						if cfg.verbose {
							fmt.Fprintf(os.Stderr, "Skipping workflow %s/%s (fully processed in DB)\n", workflow.Name, workflow.ID)
						}
						if cfg.stats != nil {
							cfg.stats.incWorkflow(true)
							if cfg.onProgress != nil {
								cfg.onProgress()
							}
						}
						hasProcessedPipeline = true
						continue
					}
				}

				// Fetch jobs with pagination
				var allJobs []WorkflowJobItem
				var jobPageToken string
				for {
					jobs, err := cfg.client.GetWorkflowJobs(ctx, workflow.ID, jobPageToken)
					if err != nil {
						cfg.warnf("⚠️  Workflow %s (%s): %v\n", workflow.Name, workflow.ID, err)
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

					handleJobWithoutDetails := func() error {
						if cfg.sqliteWriter != nil {
							if err := cfg.sqliteWriter.InsertJob(job, workflow.ID, nil, nil); err != nil {
								cfg.warnf("Warning: failed to insert job without details %s: %v\n", job.ID, err)
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
						if cfg.stats != nil {
							cfg.stats.incJob(false)
							if cfg.onProgress != nil {
								cfg.onProgress()
							}
						}
						hasProcessedPipeline = true
						return nil
					}

					if shouldSkipJobDetails(job) {
						if err := handleJobWithoutDetails(); err != nil {
							return err
						}
						continue
					}

					if cfg.sqliteWriter != nil {
						jobComplete, lookupErr := cfg.sqliteWriter.IsJobComplete(job.ID)
						if lookupErr != nil {
							cfg.warnf("Warning: DB lookup failed for job %s: %v\n", job.ID, lookupErr)
						} else if jobComplete {
							if cfg.verbose {
								fmt.Fprintf(os.Stderr, "Skipping job %s/%d (complete in DB)\n", job.Name, job.JobNumber)
							}
							if cfg.stats != nil {
								cfg.stats.incJob(true)
								if cfg.onProgress != nil {
									cfg.onProgress()
								}
							}
							hasProcessedPipeline = true
							continue
						}
					}

					jobDetails, err := cfg.client.GetJobDetails(ctx, slug, job.JobNumber)
					if err != nil {
						if isJobDetailsNotFound(err) {
							if cfg.verbose {
								cfg.warnf("Skipping job %s/%d in workflow %s: job details not found\n", job.Name, job.JobNumber, workflow.Name)
							}
							if err := handleJobWithoutDetails(); err != nil {
								return err
							}
							continue
						}
						cfg.warnf("⚠️  Job %d in workflow %s: %v\n", job.JobNumber, workflow.Name, err)
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
							cfg.warnf("Warning: failed to insert job %s: %v\n", job.ID, err)
						}
						if cfg.stats != nil {
							cfg.stats.incJob(false)
							if cfg.onProgress != nil {
								cfg.onProgress()
							}
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
							if cfg.stats != nil {
								cfg.stats.incJob(false)
								if cfg.onProgress != nil {
									cfg.onProgress()
								}
							}
							hasProcessedPipeline = true
						}
					}
				}
				if cfg.stats != nil {
					cfg.stats.incWorkflow(false)
					if cfg.onProgress != nil {
						cfg.onProgress()
					}
				}
			}

			if hasProcessedPipeline {
				if cfg.stats != nil {
					cfg.stats.incPipeline(false)
					if cfg.onProgress != nil {
						cfg.onProgress()
					}
				}
				count++
			}
		}

		if tooOld || (!cfg.timeFilterSet && count >= cfg.limit) {
			break
		}

		nextPageToken = pipelines.NextPageToken
		if nextPageToken == "" {
			break
		}
	}

	return nil
}

type sinceValue struct {
	months   int
	duration time.Duration
}

func (v sinceValue) cutoff(now time.Time) time.Time {
	if v.months > 0 {
		return now.AddDate(0, -v.months, 0)
	}
	return now.Add(-v.duration)
}

func parseSinceValue(raw string) (sinceValue, error) {
	since := strings.TrimSpace(raw)
	if since == "" {
		return sinceValue{}, fmt.Errorf("value cannot be empty")
	}

	matches := sincePattern.FindStringSubmatch(strings.ToLower(since))
	if matches != nil {
		value, err := strconv.Atoi(matches[1])
		if err != nil || value <= 0 {
			return sinceValue{}, fmt.Errorf("duration must be greater than zero")
		}

		switch matches[2] {
		case "mo", "month", "months":
			return sinceValue{months: value}, nil
		case "w", "week", "weeks":
			return sinceValue{duration: time.Duration(value) * 7 * 24 * time.Hour}, nil
		case "d", "day", "days":
			return sinceValue{duration: time.Duration(value) * 24 * time.Hour}, nil
		case "h", "hour", "hours":
			return sinceValue{duration: time.Duration(value) * time.Hour}, nil
		case "m", "min", "mins", "minute", "minutes":
			return sinceValue{duration: time.Duration(value) * time.Minute}, nil
		case "s", "sec", "secs", "second", "seconds":
			return sinceValue{duration: time.Duration(value) * time.Second}, nil
		default:
			return sinceValue{}, fmt.Errorf("unsupported unit %q (supported: month, w, day, h, m, s)", matches[2])
		}
	}

	if d, err := time.ParseDuration(since); err == nil {
		if d <= 0 {
			return sinceValue{}, fmt.Errorf("duration must be greater than zero")
		}
		return sinceValue{duration: d}, nil
	}

	return sinceValue{}, fmt.Errorf("expected formats like 1w, 1day, 1month, or 24h")
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
			&cli.StringFlag{
				Name:  "since",
				Value: "1month",
				Usage: "Relative lookback duration (e.g. 1w, 1day, 1month, 24h)",
			},
			&cli.BoolFlag{
				Name:    "verbose",
				Aliases: []string{"v"},
				Usage:   "Show detailed progress messages on stderr",
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
			since, err := parseSinceValue(c.String("since"))
			if err != nil {
				return fmt.Errorf("invalid --since value %q: %w", c.String("since"), err)
			}
			timeFilterSet := c.IsSet("since")
			cutoff := since.cutoff(time.Now())

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

			verbose := c.Bool("verbose")
			stats := &processingStats{}

			var sp *spinner.Spinner
			if format == "sqlite" && !verbose && isatty.IsTerminal(os.Stderr.Fd()) {
				sp = spinner.New(spinner.CharSets[14], 100*time.Millisecond, spinner.WithWriter(os.Stderr))
				sp.Suffix = " Processing..."
				sp.Start()
			}

			wb := &warningBuffer{buffered: sp != nil}
			client.Warnf = wb.warnf

			var onProgress func()
			if sp != nil {
				onProgress = func() {
					sp.Lock()
					sp.Suffix = stats.spinnerSuffix()
					sp.Unlock()
				}
			}

			var jobsChan chan JobQueueInfo
			var wg sync.WaitGroup
			if format != "sqlite" {
				jobsChan = make(chan JobQueueInfo)
				wg.Go(func() {
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
						client:        client,
						slug:          slug,
						limit:         limit,
						timeFilterSet: timeFilterSet,
						cutoff:        cutoff,
						verbose:       verbose,
						sqliteWriter:  sqliteWriter,
						jobsChan:      jobsChan,
						stats:         stats,
						onProgress:    onProgress,
						warnf:         wb.warnf,
					}); err != nil {
						errChan <- fmt.Errorf("❌ %v", err)
					}
				}(projectSlug)
			}

			projectWg.Wait()

			if sp != nil {
				sp.Stop()
			}

			wb.flush()

			if jobsChan != nil {
				close(jobsChan)
			}

			close(errChan)
			var errors []error
			for err := range errChan {
				errors = append(errors, err)
			}

			wg.Wait()

			fmt.Fprintf(os.Stderr, "%s\n", stats.summary())

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
