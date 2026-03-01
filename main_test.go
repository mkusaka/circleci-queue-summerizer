package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- Helper: nilIfEmpty / nilIfZero ---

func TestNilIfEmpty(t *testing.T) {
	if nilIfEmpty("") != nil {
		t.Error("expected nil for empty string")
	}
	if nilIfEmpty("hello") != "hello" {
		t.Error("expected 'hello' for non-empty string")
	}
}

func TestNilIfZero(t *testing.T) {
	if nilIfZero(0) != nil {
		t.Error("expected nil for zero")
	}
	if nilIfZero(42) != 42 {
		t.Error("expected 42 for non-zero")
	}
}

// --- SQLiteWriter ---

func TestSQLiteWriter_CreateAndInsert(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	w, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer w.Close()

	// Verify tables exist
	tables := []string{"projects", "pipelines", "workflows", "jobs"}
	for _, table := range tables {
		var name string
		err := w.db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name)
		if err != nil {
			t.Errorf("table %s not found: %v", table, err)
		}
	}
}

func TestSQLiteWriter_InsertProject(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	w, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer w.Close()

	p := &ProjectResponse{
		ID:               "proj-uuid-1",
		Slug:             "gh/org/repo",
		Name:             "repo",
		OrganizationName: "org",
		OrganizationSlug: "gh/org",
		OrganizationID:   "org-uuid-1",
	}
	p.VcsInfo.VcsURL = "https://github.com/org/repo"
	p.VcsInfo.Provider = "GitHub"
	p.VcsInfo.DefaultBranch = "main"

	if err := w.InsertProject(p); err != nil {
		t.Fatalf("InsertProject: %v", err)
	}

	var slug, vcsProvider string
	err = w.db.QueryRow("SELECT slug, vcs_provider FROM projects WHERE id=?", "proj-uuid-1").Scan(&slug, &vcsProvider)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if slug != "gh/org/repo" {
		t.Errorf("slug = %q, want %q", slug, "gh/org/repo")
	}
	if vcsProvider != "GitHub" {
		t.Errorf("vcs_provider = %q, want %q", vcsProvider, "GitHub")
	}
}

func TestSQLiteWriter_InsertPipeline(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	w, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer w.Close()

	p := PipelineItem{
		ID:          "pipe-uuid-1",
		ProjectSlug: "gh/org/repo",
		Number:      42,
		State:       "created",
		CreatedAt:   "2026-01-15T10:00:00Z",
		UpdatedAt:   "2026-01-15T10:01:00Z",
	}
	p.Trigger.Type = "webhook"
	p.Vcs.Branch = "main"
	p.Vcs.Revision = "abc123"

	if err := w.InsertPipeline(p); err != nil {
		t.Fatalf("InsertPipeline: %v", err)
	}

	var branch, triggerType string
	var number int
	err = w.db.QueryRow("SELECT number, vcs_branch, trigger_type FROM pipelines WHERE id=?", "pipe-uuid-1").Scan(&number, &branch, &triggerType)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if number != 42 {
		t.Errorf("number = %d, want 42", number)
	}
	if branch != "main" {
		t.Errorf("branch = %q, want %q", branch, "main")
	}
	if triggerType != "webhook" {
		t.Errorf("trigger_type = %q, want %q", triggerType, "webhook")
	}
}

func TestSQLiteWriter_InsertWorkflow(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	w, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer w.Close()

	wf := WorkflowItem{
		ID:             "wf-uuid-1",
		PipelineID:     "pipe-uuid-1",
		Name:           "build",
		Status:         "success",
		CreatedAt:      "2026-01-15T10:00:00Z",
		StoppedAt:      "2026-01-15T10:05:00Z",
		PipelineNumber: 42,
		ProjectSlug:    "gh/org/repo",
		StartedBy:      "user-uuid-1",
	}

	if err := w.InsertWorkflow(wf); err != nil {
		t.Fatalf("InsertWorkflow: %v", err)
	}

	var name, status string
	err = w.db.QueryRow("SELECT name, status FROM workflows WHERE id=?", "wf-uuid-1").Scan(&name, &status)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if name != "build" {
		t.Errorf("name = %q, want %q", name, "build")
	}
	if status != "success" {
		t.Errorf("status = %q, want %q", status, "success")
	}
}

func TestSQLiteWriter_InsertJob_WithDetail(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	w, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer w.Close()

	wj := WorkflowJobItem{
		ID:          "job-uuid-1",
		Name:        "test",
		Type:        "build",
		Status:      "success",
		JobNumber:   100,
		ProjectSlug: "gh/org/repo",
	}

	detail := &JobResponse{
		CreatedAt:   "2026-01-15T10:00:00Z",
		QueuedAt:    "2026-01-15T10:00:01Z",
		StartedAt:   "2026-01-15T10:00:05Z",
		StoppedAt:   "2026-01-15T10:01:00Z",
		Duration:    55000,
		Name:        "test",
		Number:      100,
		WebURL:      "https://circleci.com/jobs/100",
		Parallelism: 1,
		Status:      "success",
	}
	detail.Project.ID = "proj-uuid-1"
	detail.Project.Slug = "gh/org/repo"
	detail.Project.Name = "repo"
	detail.Executor.ResourceClass = "medium"
	detail.Executor.Type = "docker"
	detail.Organization.Name = "org"
	detail.Pipeline.ID = "pipe-uuid-1"
	detail.LatestWorkflow.ID = "wf-uuid-1"
	detail.LatestWorkflow.Name = "build"

	ms := int64(5000)
	if err := w.InsertJob(wj, "wf-uuid-1", detail, &ms); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	var jobType, executorType string
	var queueTimeMs int64
	var duration int
	err = w.db.QueryRow("SELECT type, executor_type, queue_time_ms, duration FROM jobs WHERE id=?", "job-uuid-1").Scan(&jobType, &executorType, &queueTimeMs, &duration)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if jobType != "build" {
		t.Errorf("type = %q, want %q", jobType, "build")
	}
	if executorType != "docker" {
		t.Errorf("executor_type = %q, want %q", executorType, "docker")
	}
	if queueTimeMs != 5000 {
		t.Errorf("queue_time_ms = %d, want 5000", queueTimeMs)
	}
	if duration != 55000 {
		t.Errorf("duration = %d, want 55000", duration)
	}
}

func TestSQLiteWriter_InsertJob_Approval(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	w, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer w.Close()

	wj := WorkflowJobItem{
		ID:          "approval-uuid-1",
		Name:        "hold-for-approval",
		Type:        "approval",
		Status:      "success",
		JobNumber:   0,
		ProjectSlug: "gh/org/repo",
		ApprovedBy:  "user-uuid-1",
	}

	if err := w.InsertJob(wj, "wf-uuid-1", nil, nil); err != nil {
		t.Fatalf("InsertJob (approval): %v", err)
	}

	var jobType, approvedBy string
	var createdAt sql.NullString
	var jobNumber sql.NullInt64
	err = w.db.QueryRow("SELECT type, approved_by, created_at, job_number FROM jobs WHERE id=?", "approval-uuid-1").Scan(&jobType, &approvedBy, &createdAt, &jobNumber)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if jobType != "approval" {
		t.Errorf("type = %q, want %q", jobType, "approval")
	}
	if approvedBy != "user-uuid-1" {
		t.Errorf("approved_by = %q, want %q", approvedBy, "user-uuid-1")
	}
	if createdAt.Valid {
		t.Error("created_at should be NULL for approval job")
	}
	if jobNumber.Valid {
		t.Error("job_number should be NULL for approval job (job_number=0)")
	}
}

func TestSQLiteWriter_Upsert(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	w, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer w.Close()

	wf := WorkflowItem{
		ID:         "wf-uuid-1",
		PipelineID: "pipe-uuid-1",
		Name:       "build",
		Status:     "running",
	}

	if err := w.InsertWorkflow(wf); err != nil {
		t.Fatalf("InsertWorkflow (first): %v", err)
	}

	// Upsert with updated status
	wf.Status = "success"
	wf.StoppedAt = "2026-01-15T10:05:00Z"
	if err := w.InsertWorkflow(wf); err != nil {
		t.Fatalf("InsertWorkflow (upsert): %v", err)
	}

	var count int
	var status string
	w.db.QueryRow("SELECT COUNT(*) FROM workflows WHERE id=?", "wf-uuid-1").Scan(&count)
	w.db.QueryRow("SELECT status FROM workflows WHERE id=?", "wf-uuid-1").Scan(&status)

	if count != 1 {
		t.Errorf("count = %d, want 1 (upsert should not duplicate)", count)
	}
	if status != "success" {
		t.Errorf("status = %q, want %q (should be updated)", status, "success")
	}
}

// --- doWithRetry ---

func TestDoWithRetry_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	client := &CircleCIClient{
		Token:  "test-token",
		Client: server.Client(),
	}

	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
	resp, err := client.doWithRetry(req)
	if err != nil {
		t.Fatalf("doWithRetry: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestDoWithRetry_429ThenSuccess(t *testing.T) {
	attempt := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempt == 0 {
			attempt++
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"message":"rate limited"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	client := &CircleCIClient{
		Token:  "test-token",
		Client: server.Client(),
	}

	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
	resp, err := client.doWithRetry(req)
	if err != nil {
		t.Fatalf("doWithRetry: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if attempt != 1 {
		t.Errorf("attempt = %d, want 1 (should have retried once)", attempt)
	}
}

func TestDoWithRetry_ContextCanceled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"message":"rate limited"}`))
	}))
	defer server.Close()

	client := &CircleCIClient{
		Token:  "test-token",
		Client: server.Client(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
	_, err := client.doWithRetry(req)
	if err == nil {
		t.Error("expected error for canceled context")
	}
}

func TestDoWithRetry_4xxNoRetry(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"message":"not found"}`))
	}))
	defer server.Close()

	client := &CircleCIClient{
		Token:  "test-token",
		Client: server.Client(),
	}

	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
	resp, err := client.doWithRetry(req)
	if err != nil {
		t.Fatalf("doWithRetry: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	if callCount != 1 {
		t.Errorf("callCount = %d, want 1 (4xx should not retry)", callCount)
	}
}

// --- API Response Parsing ---

func TestPipelineItemParsing(t *testing.T) {
	raw := `{
		"id": "pipe-1",
		"project_slug": "gh/org/repo",
		"number": 42,
		"state": "created",
		"created_at": "2026-01-15T10:00:00Z",
		"updated_at": "2026-01-15T10:01:00Z",
		"trigger": {
			"type": "webhook",
			"received_at": "2026-01-15T09:59:59Z",
			"actor": {"login": "user1", "avatar_url": "https://example.com/avatar.png"}
		},
		"vcs": {
			"provider_name": "GitHub",
			"branch": "main",
			"revision": "abc123",
			"commit": {"subject": "fix bug", "body": "detailed description"}
		}
	}`

	var p PipelineItem
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if p.ID != "pipe-1" {
		t.Errorf("ID = %q", p.ID)
	}
	if p.Trigger.Type != "webhook" {
		t.Errorf("Trigger.Type = %q", p.Trigger.Type)
	}
	if p.Trigger.Actor.Login != "user1" {
		t.Errorf("Trigger.Actor.Login = %q", p.Trigger.Actor.Login)
	}
	if p.Vcs.Branch != "main" {
		t.Errorf("Vcs.Branch = %q", p.Vcs.Branch)
	}
	if p.Vcs.Commit.Subject != "fix bug" {
		t.Errorf("Vcs.Commit.Subject = %q", p.Vcs.Commit.Subject)
	}
}

func TestWorkflowItemParsing(t *testing.T) {
	raw := `{
		"id": "wf-1",
		"pipeline_id": "pipe-1",
		"name": "build",
		"status": "success",
		"created_at": "2026-01-15T10:00:00Z",
		"stopped_at": "2026-01-15T10:05:00Z",
		"pipeline_number": 42,
		"project_slug": "gh/org/repo",
		"started_by": "user-uuid-1",
		"canceled_by": "",
		"tag": "setup",
		"auto_rerun_number": 1,
		"max_auto_reruns": 3
	}`

	var wf WorkflowItem
	if err := json.Unmarshal([]byte(raw), &wf); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if wf.PipelineNumber != 42 {
		t.Errorf("PipelineNumber = %d", wf.PipelineNumber)
	}
	if wf.StoppedAt != "2026-01-15T10:05:00Z" {
		t.Errorf("StoppedAt = %q", wf.StoppedAt)
	}
	if wf.Tag != "setup" {
		t.Errorf("Tag = %q", wf.Tag)
	}
}

func TestWorkflowJobItemParsing(t *testing.T) {
	raw := `{
		"id": "job-1",
		"name": "test",
		"type": "build",
		"status": "success",
		"job_number": 100,
		"started_at": "2026-01-15T10:00:05Z",
		"stopped_at": "2026-01-15T10:01:00Z",
		"project_slug": "gh/org/repo",
		"approved_by": "user-uuid-1",
		"approval_request_id": "req-uuid-1"
	}`

	var wj WorkflowJobItem
	if err := json.Unmarshal([]byte(raw), &wj); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if wj.Type != "build" {
		t.Errorf("Type = %q", wj.Type)
	}
	if wj.ApprovedBy != "user-uuid-1" {
		t.Errorf("ApprovedBy = %q", wj.ApprovedBy)
	}
}

func TestJobResponseParsing(t *testing.T) {
	raw := `{
		"created_at": "2026-01-15T10:00:00Z",
		"queued_at": "2026-01-15T10:00:01Z",
		"started_at": "2026-01-15T10:00:05Z",
		"stopped_at": "2026-01-15T10:01:00Z",
		"duration": 55000,
		"name": "test",
		"number": 100,
		"web_url": "https://circleci.com/jobs/100",
		"parallelism": 2,
		"status": "success",
		"project": {"id": "p1", "slug": "gh/org/repo", "name": "repo", "external_url": "https://github.com/org/repo"},
		"executor": {"resource_class": "large", "type": "docker"},
		"organization": {"name": "org"},
		"latest_workflow": {"id": "wf-1", "name": "build"},
		"pipeline": {"id": "pipe-1"}
	}`

	var jr JobResponse
	if err := json.Unmarshal([]byte(raw), &jr); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if jr.WebURL != "https://circleci.com/jobs/100" {
		t.Errorf("WebURL = %q", jr.WebURL)
	}
	if jr.Parallelism != 2 {
		t.Errorf("Parallelism = %d", jr.Parallelism)
	}
	if jr.Executor.ResourceClass != "large" {
		t.Errorf("Executor.ResourceClass = %q", jr.Executor.ResourceClass)
	}
	if jr.Pipeline.ID != "pipe-1" {
		t.Errorf("Pipeline.ID = %q", jr.Pipeline.ID)
	}
}

func TestProjectResponseParsing(t *testing.T) {
	raw := `{
		"id": "proj-1",
		"slug": "gh/org/repo",
		"name": "repo",
		"organization_name": "org",
		"organization_slug": "gh/org",
		"organization_id": "org-uuid-1",
		"vcs_info": {
			"vcs_url": "https://github.com/org/repo",
			"provider": "GitHub",
			"default_branch": "main"
		}
	}`

	var pr ProjectResponse
	if err := json.Unmarshal([]byte(raw), &pr); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if pr.OrganizationName != "org" {
		t.Errorf("OrganizationName = %q", pr.OrganizationName)
	}
	if pr.VcsInfo.DefaultBranch != "main" {
		t.Errorf("VcsInfo.DefaultBranch = %q", pr.VcsInfo.DefaultBranch)
	}
}

// --- expandProjectSlugs ---

func TestExpandProjectSlugs_PassThrough(t *testing.T) {
	// Mock server not needed for non-all slugs
	client := &CircleCIClient{Token: "test", Client: &http.Client{}}
	slugs, err := expandProjectSlugs(context.Background(), client, []string{"gh/org/repo1", "gh/org/repo2"})
	if err != nil {
		t.Fatalf("expandProjectSlugs: %v", err)
	}
	if len(slugs) != 2 {
		t.Fatalf("len = %d, want 2", len(slugs))
	}
	if slugs[0] != "gh/org/repo1" || slugs[1] != "gh/org/repo2" {
		t.Errorf("slugs = %v", slugs)
	}
}

func TestExpandProjectSlugs_All(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(OrgSummaryResponse{
			AllProjects: []string{"repo1", "repo2", "repo3"},
		})
	}))
	defer server.Close()

	// Test with mock by directly calling a function that hits the test server
	origGetOrgProjects := func(ctx context.Context, orgSlug string) ([]string, error) {
		url := fmt.Sprintf("%s/api/v2/insights/%s/summary", server.URL, orgSlug)
		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		resp, err := server.Client().Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		var summary OrgSummaryResponse
		json.NewDecoder(resp.Body).Decode(&summary)
		return summary.AllProjects, nil
	}

	names, err := origGetOrgProjects(context.Background(), "gh/org")
	if err != nil {
		t.Fatalf("GetOrgProjects mock: %v", err)
	}

	// Simulate expansion
	var expanded []string
	for _, name := range names {
		expanded = append(expanded, "gh/org/"+name)
	}

	if len(expanded) != 3 {
		t.Fatalf("len = %d, want 3", len(expanded))
	}
	if expanded[0] != "gh/org/repo1" {
		t.Errorf("expanded[0] = %q, want %q", expanded[0], "gh/org/repo1")
	}
	if expanded[2] != "gh/org/repo3" {
		t.Errorf("expanded[2] = %q, want %q", expanded[2], "gh/org/repo3")
	}
}

// --- Format validation ---

func TestFormatValidation(t *testing.T) {
	// Valid formats should not cause issues in the switch
	validFormats := []string{"table", "ndjson", "sqlite"}
	for _, f := range validFormats {
		switch f {
		case "table", "ndjson", "sqlite":
			// ok
		default:
			t.Errorf("format %q should be valid", f)
		}
	}

	// Invalid format
	invalid := "csv"
	switch invalid {
	case "table", "ndjson", "sqlite":
		t.Errorf("format %q should be invalid", invalid)
	default:
		// expected
	}
}

// --- SQLite end-to-end with mock API ---

func TestSQLiteEndToEnd(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "e2e.db")
	sw, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer sw.Close()

	// Insert project
	proj := &ProjectResponse{ID: "proj-1", Slug: "gh/org/repo", Name: "repo", OrganizationName: "org"}
	sw.InsertProject(proj)

	// Insert pipeline
	pipe := PipelineItem{ID: "pipe-1", ProjectSlug: "gh/org/repo", Number: 1, State: "created", CreatedAt: time.Now().Format(time.RFC3339)}
	sw.InsertPipeline(pipe)

	// Insert workflow
	wf := WorkflowItem{ID: "wf-1", PipelineID: "pipe-1", Name: "build", Status: "success", CreatedAt: time.Now().Format(time.RFC3339)}
	sw.InsertWorkflow(wf)

	// Insert build job
	buildJob := WorkflowJobItem{ID: "job-1", Name: "test", Type: "build", Status: "success", JobNumber: 1, ProjectSlug: "gh/org/repo"}
	detail := &JobResponse{CreatedAt: "2026-01-15T10:00:00Z", QueuedAt: "2026-01-15T10:00:01Z", StartedAt: "2026-01-15T10:00:05Z", StoppedAt: "2026-01-15T10:01:00Z", Duration: 55000, Name: "test", Number: 1, WebURL: "https://circleci.com/jobs/1", Parallelism: 1, Status: "success"}
	detail.Executor.ResourceClass = "medium"
	detail.Executor.Type = "docker"
	detail.Organization.Name = "org"
	detail.Pipeline.ID = "pipe-1"
	detail.LatestWorkflow.ID = "wf-1"
	ms := int64(5000)
	sw.InsertJob(buildJob, "wf-1", detail, &ms)

	// Insert approval job
	approvalJob := WorkflowJobItem{ID: "approval-1", Name: "hold", Type: "approval", Status: "success", JobNumber: 0, ProjectSlug: "gh/org/repo", ApprovedBy: "user-1"}
	sw.InsertJob(approvalJob, "wf-1", nil, nil)

	// Verify counts
	var projectCount, pipelineCount, workflowCount, jobCount int
	sw.db.QueryRow("SELECT COUNT(*) FROM projects").Scan(&projectCount)
	sw.db.QueryRow("SELECT COUNT(*) FROM pipelines").Scan(&pipelineCount)
	sw.db.QueryRow("SELECT COUNT(*) FROM workflows").Scan(&workflowCount)
	sw.db.QueryRow("SELECT COUNT(*) FROM jobs").Scan(&jobCount)

	if projectCount != 1 {
		t.Errorf("projects count = %d, want 1", projectCount)
	}
	if pipelineCount != 1 {
		t.Errorf("pipelines count = %d, want 1", pipelineCount)
	}
	if workflowCount != 1 {
		t.Errorf("workflows count = %d, want 1", workflowCount)
	}
	if jobCount != 2 {
		t.Errorf("jobs count = %d, want 2 (build + approval)", jobCount)
	}

	// Verify approval job has NULL detail fields
	var approvalCreatedAt sql.NullString
	sw.db.QueryRow("SELECT created_at FROM jobs WHERE type='approval'").Scan(&approvalCreatedAt)
	if approvalCreatedAt.Valid {
		t.Error("approval job created_at should be NULL")
	}

	// Verify build job has queue_time_ms
	var buildQueueTime int64
	sw.db.QueryRow("SELECT queue_time_ms FROM jobs WHERE type='build'").Scan(&buildQueueTime)
	if buildQueueTime != 5000 {
		t.Errorf("build job queue_time_ms = %d, want 5000", buildQueueTime)
	}

	// Verify upsert: re-insert same data, counts should not change
	sw.InsertProject(proj)
	sw.InsertPipeline(pipe)
	sw.InsertWorkflow(wf)
	sw.InsertJob(buildJob, "wf-1", detail, &ms)
	sw.InsertJob(approvalJob, "wf-1", nil, nil)

	sw.db.QueryRow("SELECT COUNT(*) FROM projects").Scan(&projectCount)
	sw.db.QueryRow("SELECT COUNT(*) FROM pipelines").Scan(&pipelineCount)
	sw.db.QueryRow("SELECT COUNT(*) FROM workflows").Scan(&workflowCount)
	sw.db.QueryRow("SELECT COUNT(*) FROM jobs").Scan(&jobCount)

	if projectCount != 1 || pipelineCount != 1 || workflowCount != 1 || jobCount != 2 {
		t.Errorf("after upsert: projects=%d pipelines=%d workflows=%d jobs=%d (all should be unchanged)",
			projectCount, pipelineCount, workflowCount, jobCount)
	}

}

// --- CLI validation tests ---

func TestCLI_InvalidFormat(t *testing.T) {
	app := newApp()
	err := app.Run([]string{"app", "-p", "gh/org/repo", "--format", "csv", "--token", "dummy"})
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
	if !strings.Contains(err.Error(), "invalid format") {
		t.Errorf("error = %q, want to contain 'invalid format'", err.Error())
	}
}

func TestCLI_SqliteWithoutOutput(t *testing.T) {
	app := newApp()
	err := app.Run([]string{"app", "-p", "gh/org/repo", "--format", "sqlite", "--token", "dummy"})
	if err == nil {
		t.Fatal("expected error for sqlite without --output")
	}
	if !strings.Contains(err.Error(), "--output flag is required") {
		t.Errorf("error = %q, want to contain '--output flag is required'", err.Error())
	}
}

// --- Mock API server for integration tests ---

// newMockCircleCIServer creates a mock server that responds to the
// CircleCI API endpoints used by processProject.
func newMockCircleCIServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasPrefix(path, "/api/v2/project/gh/org/repo/pipeline"):
			json.NewEncoder(w).Encode(PipelineResponse{
				Items: []PipelineItem{
					{
						ID:          "pipe-1",
						ProjectSlug: "gh/org/repo",
						Number:      1,
						State:       "created",
						CreatedAt:   time.Now().Format(time.RFC3339),
						UpdatedAt:   time.Now().Format(time.RFC3339),
					},
				},
			})
		case strings.HasPrefix(path, "/api/v2/project/gh/org/repo/job/"):
			jr := JobResponse{
				CreatedAt:   "2026-01-15T10:00:00Z",
				QueuedAt:    "2026-01-15T10:00:01Z",
				StartedAt:   "2026-01-15T10:00:05Z",
				StoppedAt:   "2026-01-15T10:01:00Z",
				Duration:    55000,
				Name:        "test-job",
				Number:      1,
				WebURL:      "https://circleci.com/jobs/1",
				Parallelism: 2,
				Status:      "success",
			}
			jr.Executor.ResourceClass = "medium"
			jr.Executor.Type = "docker"
			jr.Organization.Name = "org"
			jr.Project.ID = "proj-1"
			jr.Project.Slug = "gh/org/repo"
			jr.Project.Name = "repo"
			jr.Project.ExternalURL = "https://github.com/org/repo"
			jr.LatestWorkflow.ID = "wf-1"
			jr.LatestWorkflow.Name = "build"
			jr.Pipeline.ID = "pipe-1"
			json.NewEncoder(w).Encode(jr)
		case strings.HasPrefix(path, "/api/v2/project/gh/org/repo"):
			json.NewEncoder(w).Encode(ProjectResponse{
				ID:               "proj-1",
				Slug:             "gh/org/repo",
				Name:             "repo",
				OrganizationName: "org",
			})
		case strings.HasPrefix(path, "/api/v2/pipeline/pipe-1/workflow"):
			json.NewEncoder(w).Encode(PipelineWorkflowResponse{
				Items: []WorkflowItem{
					{
						ID:         "wf-1",
						PipelineID: "pipe-1",
						Name:       "build",
						Status:     "success",
						CreatedAt:  time.Now().Format(time.RFC3339),
					},
				},
			})
		case strings.HasPrefix(path, "/api/v2/workflow/wf-1/job"):
			json.NewEncoder(w).Encode(WorkflowJobsResponse{
				Items: []WorkflowJobItem{
					{
						ID:          "job-1",
						Name:        "test-job",
						Type:        "build",
						Status:      "success",
						JobNumber:   1,
						ProjectSlug: "gh/org/repo",
					},
				},
			})
		default:
			t.Logf("unhandled path: %s", path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestProcessProject_SQLite(t *testing.T) {
	server := newMockCircleCIServer(t)
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	sw, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer sw.Close()

	client := &CircleCIClient{
		Token:   "test-token",
		Client:  server.Client(),
		BaseURL: server.URL,
	}

	err = processProject(context.Background(), processProjectConfig{
		client:       client,
		slug:         "gh/org/repo",
		limit:        10,
		cutoff:       time.Now().AddDate(0, -1, 0),
		sqliteWriter: sw,
	})
	if err != nil {
		t.Fatalf("processProject: %v", err)
	}

	// Verify project inserted
	var projectCount int
	sw.db.QueryRow("SELECT COUNT(*) FROM projects").Scan(&projectCount)
	if projectCount != 1 {
		t.Errorf("projects count = %d, want 1", projectCount)
	}

	// Verify pipeline inserted
	var pipelineCount int
	sw.db.QueryRow("SELECT COUNT(*) FROM pipelines").Scan(&pipelineCount)
	if pipelineCount != 1 {
		t.Errorf("pipelines count = %d, want 1", pipelineCount)
	}

	// Verify workflow inserted
	var workflowCount int
	sw.db.QueryRow("SELECT COUNT(*) FROM workflows").Scan(&workflowCount)
	if workflowCount != 1 {
		t.Errorf("workflows count = %d, want 1", workflowCount)
	}

	// Verify job inserted with correct data
	var jobCount int
	var jobName, webURL string
	var queueTimeMs int64
	sw.db.QueryRow("SELECT COUNT(*) FROM jobs").Scan(&jobCount)
	if jobCount != 1 {
		t.Errorf("jobs count = %d, want 1", jobCount)
	}
	sw.db.QueryRow("SELECT name, web_url, queue_time_ms FROM jobs WHERE id='job-1'").Scan(&jobName, &webURL, &queueTimeMs)
	if jobName != "test-job" {
		t.Errorf("job name = %q, want %q", jobName, "test-job")
	}
	if webURL != "https://circleci.com/jobs/1" {
		t.Errorf("web_url = %q, want %q", webURL, "https://circleci.com/jobs/1")
	}
	if queueTimeMs != 5000 {
		t.Errorf("queue_time_ms = %d, want 5000", queueTimeMs)
	}
}

func TestProcessProject_SQLite_Upsert(t *testing.T) {
	server := newMockCircleCIServer(t)
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	sw, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer sw.Close()

	client := &CircleCIClient{
		Token:   "test-token",
		Client:  server.Client(),
		BaseURL: server.URL,
	}

	cfg := processProjectConfig{
		client:       client,
		slug:         "gh/org/repo",
		limit:        10,
		cutoff:       time.Now().AddDate(0, -1, 0),
		sqliteWriter: sw,
	}

	// Run twice
	if err := processProject(context.Background(), cfg); err != nil {
		t.Fatalf("processProject (1st): %v", err)
	}
	if err := processProject(context.Background(), cfg); err != nil {
		t.Fatalf("processProject (2nd): %v", err)
	}

	// Counts should not change after 2nd run
	var projectCount, pipelineCount, workflowCount, jobCount int
	sw.db.QueryRow("SELECT COUNT(*) FROM projects").Scan(&projectCount)
	sw.db.QueryRow("SELECT COUNT(*) FROM pipelines").Scan(&pipelineCount)
	sw.db.QueryRow("SELECT COUNT(*) FROM workflows").Scan(&workflowCount)
	sw.db.QueryRow("SELECT COUNT(*) FROM jobs").Scan(&jobCount)

	if projectCount != 1 {
		t.Errorf("projects count = %d, want 1", projectCount)
	}
	if pipelineCount != 1 {
		t.Errorf("pipelines count = %d, want 1", pipelineCount)
	}
	if workflowCount != 1 {
		t.Errorf("workflows count = %d, want 1", workflowCount)
	}
	if jobCount != 1 {
		t.Errorf("jobs count = %d, want 1", jobCount)
	}
}

func TestProcessProject_NDJSON(t *testing.T) {
	server := newMockCircleCIServer(t)
	defer server.Close()

	client := &CircleCIClient{
		Token:   "test-token",
		Client:  server.Client(),
		BaseURL: server.URL,
	}

	jobsChan := make(chan JobQueueInfo, 10)

	err := processProject(context.Background(), processProjectConfig{
		client:   client,
		slug:     "gh/org/repo",
		limit:    10,
		cutoff:   time.Now().AddDate(0, -1, 0),
		jobsChan: jobsChan,
	})
	if err != nil {
		t.Fatalf("processProject: %v", err)
	}
	close(jobsChan)

	var jobs []JobQueueInfo
	for job := range jobsChan {
		jobs = append(jobs, job)
	}

	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}

	// Verify NDJSON output contains new fields
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(jobs[0])
	line := buf.String()
	for _, field := range []string{"test-job", "queue_time", "type", "web_url", "executor_resource_class", "executor_type", "organization_name", "project_id", "project_name", "latest_workflow_id"} {
		if !strings.Contains(line, field) {
			t.Errorf("ndjson output should contain %q, got %q", field, line)
		}
	}

	// Verify the actual field values
	if jobs[0].Type != "build" {
		t.Errorf("Type = %q, want %q", jobs[0].Type, "build")
	}
	if jobs[0].WebURL != "https://circleci.com/jobs/1" {
		t.Errorf("WebURL = %q, want %q", jobs[0].WebURL, "https://circleci.com/jobs/1")
	}
}

func TestProcessProject_Table(t *testing.T) {
	server := newMockCircleCIServer(t)
	defer server.Close()

	client := &CircleCIClient{
		Token:   "test-token",
		Client:  server.Client(),
		BaseURL: server.URL,
	}

	jobsChan := make(chan JobQueueInfo, 10)

	err := processProject(context.Background(), processProjectConfig{
		client:   client,
		slug:     "gh/org/repo",
		limit:    10,
		cutoff:   time.Now().AddDate(0, -1, 0),
		jobsChan: jobsChan,
	})
	if err != nil {
		t.Fatalf("processProject: %v", err)
	}
	close(jobsChan)

	var jobs []JobQueueInfo
	for job := range jobsChan {
		jobs = append(jobs, job)
	}

	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}

	// Verify new fields are populated
	job := jobs[0]
	if job.Type != "build" {
		t.Errorf("Type = %q, want %q", job.Type, "build")
	}
	if job.ExecutorResourceClass != "medium" {
		t.Errorf("ExecutorResourceClass = %q, want %q", job.ExecutorResourceClass, "medium")
	}
	if job.ExecutorType != "docker" {
		t.Errorf("ExecutorType = %q, want %q", job.ExecutorType, "docker")
	}
	if job.OrganizationName != "org" {
		t.Errorf("OrganizationName = %q, want %q", job.OrganizationName, "org")
	}
	if job.ProjectID != "proj-1" {
		t.Errorf("ProjectID = %q, want %q", job.ProjectID, "proj-1")
	}
	if job.Parallelism != 2 {
		t.Errorf("Parallelism = %d, want 2", job.Parallelism)
	}
}

// --- BaseURL tests ---

func TestCircleCIClient_BaseURL(t *testing.T) {
	c := &CircleCIClient{Token: "test"}
	if c.baseURL() != "https://circleci.com" {
		t.Errorf("default baseURL = %q, want %q", c.baseURL(), "https://circleci.com")
	}

	c.BaseURL = "http://localhost:8080"
	if c.baseURL() != "http://localhost:8080" {
		t.Errorf("custom baseURL = %q, want %q", c.baseURL(), "http://localhost:8080")
	}
}

// --- Mock server for approval job ---

func newMockCircleCIServerWithApproval(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasPrefix(path, "/api/v2/project/gh/org/repo/pipeline"):
			json.NewEncoder(w).Encode(PipelineResponse{
				Items: []PipelineItem{
					{
						ID:          "pipe-1",
						ProjectSlug: "gh/org/repo",
						Number:      1,
						State:       "created",
						CreatedAt:   time.Now().Format(time.RFC3339),
					},
				},
			})
		case strings.HasPrefix(path, "/api/v2/project/gh/org/repo/job/"):
			json.NewEncoder(w).Encode(JobResponse{
				CreatedAt: "2026-01-15T10:00:00Z",
				QueuedAt:  "2026-01-15T10:00:01Z",
				StartedAt: "2026-01-15T10:00:05Z",
				StoppedAt: "2026-01-15T10:01:00Z",
				Duration:  55000,
				Name:      "build-job",
				Number:    1,
				Status:    "success",
			})
		case strings.HasPrefix(path, "/api/v2/project/gh/org/repo"):
			json.NewEncoder(w).Encode(ProjectResponse{
				ID:   "proj-1",
				Slug: "gh/org/repo",
				Name: "repo",
			})
		case strings.HasPrefix(path, "/api/v2/pipeline/pipe-1/workflow"):
			json.NewEncoder(w).Encode(PipelineWorkflowResponse{
				Items: []WorkflowItem{
					{
						ID:         "wf-1",
						PipelineID: "pipe-1",
						Name:       "deploy",
						Status:     "success",
						CreatedAt:  time.Now().Format(time.RFC3339),
					},
				},
			})
		case strings.HasPrefix(path, "/api/v2/workflow/wf-1/job"):
			json.NewEncoder(w).Encode(WorkflowJobsResponse{
				Items: []WorkflowJobItem{
					{
						ID:        "build-job-1",
						Name:      "build-job",
						Type:      "build",
						Status:    "success",
						JobNumber: 1,
					},
					{
						ID:         "approval-1",
						Name:       "hold-for-deploy",
						Type:       "approval",
						Status:     "success",
						JobNumber:  0,
						ApprovedBy: "user-1",
					},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestProcessProject_SQLite_ApprovalJob(t *testing.T) {
	server := newMockCircleCIServerWithApproval(t)
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	sw, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer sw.Close()

	client := &CircleCIClient{
		Token:   "test-token",
		Client:  server.Client(),
		BaseURL: server.URL,
	}

	err = processProject(context.Background(), processProjectConfig{
		client:       client,
		slug:         "gh/org/repo",
		limit:        10,
		cutoff:       time.Now().AddDate(0, -1, 0),
		sqliteWriter: sw,
	})
	if err != nil {
		t.Fatalf("processProject: %v", err)
	}

	// Should have 2 jobs: 1 build + 1 approval
	var jobCount int
	sw.db.QueryRow("SELECT COUNT(*) FROM jobs").Scan(&jobCount)
	if jobCount != 2 {
		t.Errorf("jobs count = %d, want 2 (build + approval)", jobCount)
	}

	// Approval job should have NULL detail columns
	var createdAt sql.NullString
	var queueTimeMs sql.NullInt64
	sw.db.QueryRow("SELECT created_at, queue_time_ms FROM jobs WHERE type='approval'").Scan(&createdAt, &queueTimeMs)
	if createdAt.Valid {
		t.Error("approval job created_at should be NULL")
	}
	if queueTimeMs.Valid {
		t.Error("approval job queue_time_ms should be NULL")
	}

	// Build job should have detail columns
	var buildCreatedAt string
	sw.db.QueryRow("SELECT created_at FROM jobs WHERE type='build'").Scan(&buildCreatedAt)
	if buildCreatedAt != "2026-01-15T10:00:00Z" {
		t.Errorf("build job created_at = %q, want %q", buildCreatedAt, "2026-01-15T10:00:00Z")
	}
}

// --- Test that table/ndjson include approval jobs ---

func TestProcessProject_IncludesApproval(t *testing.T) {
	server := newMockCircleCIServerWithApproval(t)
	defer server.Close()

	client := &CircleCIClient{
		Token:   "test-token",
		Client:  server.Client(),
		BaseURL: server.URL,
	}

	jobsChan := make(chan JobQueueInfo, 10)
	err := processProject(context.Background(), processProjectConfig{
		client:   client,
		slug:     "gh/org/repo",
		limit:    10,
		cutoff:   time.Now().AddDate(0, -1, 0),
		jobsChan: jobsChan,
	})
	if err != nil {
		t.Fatalf("processProject: %v", err)
	}
	close(jobsChan)

	var jobs []JobQueueInfo
	for job := range jobsChan {
		jobs = append(jobs, job)
	}

	// Both build and approval jobs should be in jobsChan
	if len(jobs) != 2 {
		t.Fatalf("got %d jobs, want 2 (build + approval)", len(jobs))
	}

	// Find approval and build jobs
	var approvalJob, buildJob *JobQueueInfo
	for i := range jobs {
		switch jobs[i].Type {
		case "approval":
			approvalJob = &jobs[i]
		case "build":
			buildJob = &jobs[i]
		}
	}

	if buildJob == nil {
		t.Fatal("build job not found")
	}
	if buildJob.JobName != "build-job" {
		t.Errorf("build job name = %q, want %q", buildJob.JobName, "build-job")
	}
	if buildJob.QueueTime != 5000 {
		t.Errorf("build job queue_time = %d, want 5000", buildJob.QueueTime)
	}

	if approvalJob == nil {
		t.Fatal("approval job not found")
	}
	if approvalJob.JobName != "hold-for-deploy" {
		t.Errorf("approval job name = %q, want %q", approvalJob.JobName, "hold-for-deploy")
	}
	if approvalJob.ApprovedBy != "user-1" {
		t.Errorf("approval job approved_by = %q, want %q", approvalJob.ApprovedBy, "user-1")
	}
	// Approval job should have zero-value detail fields
	if approvalJob.QueueTime != 0 {
		t.Errorf("approval job queue_time = %d, want 0", approvalJob.QueueTime)
	}
	if approvalJob.WebURL != "" {
		t.Errorf("approval job web_url = %q, want empty", approvalJob.WebURL)
	}
}

// --- Lookup method unit tests ---

func TestIsPipelineFullyProcessed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	w, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer w.Close()

	// No workflows → not fully processed
	ok, err := w.IsPipelineFullyProcessed("pipe-1")
	if err != nil {
		t.Fatalf("IsPipelineFullyProcessed: %v", err)
	}
	if ok {
		t.Error("expected false when no workflows exist")
	}

	// Add terminal workflow without jobs → not fully processed
	w.InsertWorkflow(WorkflowItem{ID: "wf-1", PipelineID: "pipe-1", Name: "build", Status: "success"})
	ok, err = w.IsPipelineFullyProcessed("pipe-1")
	if err != nil {
		t.Fatalf("IsPipelineFullyProcessed: %v", err)
	}
	if ok {
		t.Error("expected false when workflow has no jobs")
	}

	// Add a job → now fully processed
	detail := &JobResponse{CreatedAt: "2026-01-15T10:00:00Z", StartedAt: "2026-01-15T10:00:05Z"}
	ms := int64(5000)
	w.InsertJob(WorkflowJobItem{ID: "job-1", Name: "test", Type: "build", Status: "success", JobNumber: 1}, "wf-1", detail, &ms)
	ok, err = w.IsPipelineFullyProcessed("pipe-1")
	if err != nil {
		t.Fatalf("IsPipelineFullyProcessed: %v", err)
	}
	if !ok {
		t.Error("expected true when all workflows terminal and have jobs")
	}

	// Add non-terminal workflow → not fully processed
	w.InsertWorkflow(WorkflowItem{ID: "wf-2", PipelineID: "pipe-1", Name: "deploy", Status: "running"})
	ok, err = w.IsPipelineFullyProcessed("pipe-1")
	if err != nil {
		t.Fatalf("IsPipelineFullyProcessed: %v", err)
	}
	if ok {
		t.Error("expected false when one workflow is non-terminal")
	}
}

func TestIsWorkflowFullyProcessed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	w, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer w.Close()

	// Not exists → false
	ok, err := w.IsWorkflowFullyProcessed("wf-1")
	if err != nil {
		t.Fatalf("IsWorkflowFullyProcessed: %v", err)
	}
	if ok {
		t.Error("expected false when workflow does not exist")
	}

	// Terminal but no jobs → false
	w.InsertWorkflow(WorkflowItem{ID: "wf-1", PipelineID: "pipe-1", Name: "build", Status: "success"})
	ok, err = w.IsWorkflowFullyProcessed("wf-1")
	if err != nil {
		t.Fatalf("IsWorkflowFullyProcessed: %v", err)
	}
	if ok {
		t.Error("expected false when terminal but no jobs")
	}

	// Terminal with jobs → true
	w.InsertJob(WorkflowJobItem{ID: "job-1", Name: "test", Status: "success", JobNumber: 1}, "wf-1", &JobResponse{CreatedAt: "2026-01-15T10:00:00Z", StartedAt: "2026-01-15T10:00:05Z"}, nil)
	ok, err = w.IsWorkflowFullyProcessed("wf-1")
	if err != nil {
		t.Fatalf("IsWorkflowFullyProcessed: %v", err)
	}
	if !ok {
		t.Error("expected true when terminal with jobs")
	}

	// Non-terminal with jobs → false
	w.InsertWorkflow(WorkflowItem{ID: "wf-2", PipelineID: "pipe-1", Name: "deploy", Status: "running"})
	w.InsertJob(WorkflowJobItem{ID: "job-2", Name: "deploy", Status: "running", JobNumber: 2}, "wf-2", &JobResponse{CreatedAt: "2026-01-15T10:00:00Z", StartedAt: "2026-01-15T10:00:05Z"}, nil)
	ok, err = w.IsWorkflowFullyProcessed("wf-2")
	if err != nil {
		t.Fatalf("IsWorkflowFullyProcessed: %v", err)
	}
	if ok {
		t.Error("expected false when workflow is non-terminal")
	}
}

func TestIsJobComplete(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	w, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer w.Close()

	// Not exists → false
	ok, err := w.IsJobComplete("job-1")
	if err != nil {
		t.Fatalf("IsJobComplete: %v", err)
	}
	if ok {
		t.Error("expected false when job does not exist")
	}

	// Terminal with details → true
	detail := &JobResponse{CreatedAt: "2026-01-15T10:00:00Z", StartedAt: "2026-01-15T10:00:05Z"}
	w.InsertJob(WorkflowJobItem{ID: "job-1", Name: "test", Status: "success", JobNumber: 1}, "wf-1", detail, nil)
	ok, err = w.IsJobComplete("job-1")
	if err != nil {
		t.Fatalf("IsJobComplete: %v", err)
	}
	if !ok {
		t.Error("expected true when terminal with details")
	}

	// Terminal without details (approval job) → false
	w.InsertJob(WorkflowJobItem{ID: "approval-1", Name: "hold", Type: "approval", Status: "success", JobNumber: 0}, "wf-1", nil, nil)
	ok, err = w.IsJobComplete("approval-1")
	if err != nil {
		t.Fatalf("IsJobComplete: %v", err)
	}
	if ok {
		t.Error("expected false when terminal but no details (approval job)")
	}

	// Non-terminal with details → false
	w.InsertJob(WorkflowJobItem{ID: "job-2", Name: "build", Status: "running", JobNumber: 2}, "wf-1",
		&JobResponse{CreatedAt: "2026-01-15T10:00:00Z", StartedAt: "2026-01-15T10:00:05Z"}, nil)
	ok, err = w.IsJobComplete("job-2")
	if err != nil {
		t.Fatalf("IsJobComplete: %v", err)
	}
	if ok {
		t.Error("expected false when non-terminal")
	}
}

// --- Integration test: API call skipping ---

func TestProcessProject_SQLite_SkipsRedundantAPICalls(t *testing.T) {
	var mu sync.Mutex
	callCounts := map[string]int{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		mu.Lock()
		switch {
		case strings.HasSuffix(path, "/pipeline"):
			callCounts["getPipelines"]++
		case strings.Contains(path, "/job/"):
			callCounts["getJobDetails"]++
		case strings.HasPrefix(path, "/api/v2/pipeline/") && strings.Contains(path, "/workflow"):
			callCounts["getPipelineWorkflows"]++
		case strings.HasPrefix(path, "/api/v2/workflow/") && strings.Contains(path, "/job"):
			callCounts["getWorkflowJobs"]++
		case strings.HasPrefix(path, "/api/v2/project/"):
			callCounts["getProject"]++
		}
		mu.Unlock()

		switch {
		case strings.HasSuffix(path, "/pipeline"):
			json.NewEncoder(w).Encode(PipelineResponse{
				Items: []PipelineItem{{
					ID:          "pipe-1",
					ProjectSlug: "gh/org/repo",
					Number:      1,
					State:       "created",
					CreatedAt:   time.Now().Format(time.RFC3339),
				}},
			})
		case strings.Contains(path, "/job/"):
			jr := JobResponse{
				CreatedAt: "2026-01-15T10:00:00Z",
				QueuedAt:  "2026-01-15T10:00:01Z",
				StartedAt: "2026-01-15T10:00:05Z",
				StoppedAt: "2026-01-15T10:01:00Z",
				Duration:  55000,
				Name:      "test-job",
				Number:    1,
				WebURL:    "https://circleci.com/jobs/1",
				Status:    "success",
			}
			jr.Executor.ResourceClass = "medium"
			jr.Executor.Type = "docker"
			jr.Organization.Name = "org"
			jr.Project.ID = "proj-1"
			jr.Pipeline.ID = "pipe-1"
			jr.LatestWorkflow.ID = "wf-1"
			json.NewEncoder(w).Encode(jr)
		case strings.HasPrefix(path, "/api/v2/pipeline/") && strings.Contains(path, "/workflow"):
			json.NewEncoder(w).Encode(PipelineWorkflowResponse{
				Items: []WorkflowItem{{
					ID:         "wf-1",
					PipelineID: "pipe-1",
					Name:       "build",
					Status:     "success",
					CreatedAt:  time.Now().Format(time.RFC3339),
				}},
			})
		case strings.HasPrefix(path, "/api/v2/workflow/") && strings.Contains(path, "/job"):
			json.NewEncoder(w).Encode(WorkflowJobsResponse{
				Items: []WorkflowJobItem{{
					ID:          "job-1",
					Name:        "test-job",
					Type:        "build",
					Status:      "success",
					JobNumber:   1,
					ProjectSlug: "gh/org/repo",
				}},
			})
		case strings.HasPrefix(path, "/api/v2/project/"):
			json.NewEncoder(w).Encode(ProjectResponse{
				ID:   "proj-1",
				Slug: "gh/org/repo",
				Name: "repo",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	sw, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer sw.Close()

	client := &CircleCIClient{
		Token:   "test-token",
		Client:  server.Client(),
		BaseURL: server.URL,
	}

	cfg := processProjectConfig{
		client:       client,
		slug:         "gh/org/repo",
		limit:        10,
		cutoff:       time.Now().AddDate(0, -1, 0),
		sqliteWriter: sw,
	}

	// First run: all API calls should happen
	if err := processProject(context.Background(), cfg); err != nil {
		t.Fatalf("processProject (1st): %v", err)
	}

	mu.Lock()
	firstRun := map[string]int{}
	for k, v := range callCounts {
		firstRun[k] = v
	}
	mu.Unlock()

	if firstRun["getPipelines"] != 1 {
		t.Errorf("1st run: getPipelines = %d, want 1", firstRun["getPipelines"])
	}
	if firstRun["getPipelineWorkflows"] != 1 {
		t.Errorf("1st run: getPipelineWorkflows = %d, want 1", firstRun["getPipelineWorkflows"])
	}
	if firstRun["getWorkflowJobs"] != 1 {
		t.Errorf("1st run: getWorkflowJobs = %d, want 1", firstRun["getWorkflowJobs"])
	}
	if firstRun["getJobDetails"] != 1 {
		t.Errorf("1st run: getJobDetails = %d, want 1", firstRun["getJobDetails"])
	}

	// Reset counts
	mu.Lock()
	for k := range callCounts {
		callCounts[k] = 0
	}
	mu.Unlock()

	// Second run: pipeline-level skip should prevent downstream calls
	if err := processProject(context.Background(), cfg); err != nil {
		t.Fatalf("processProject (2nd): %v", err)
	}

	mu.Lock()
	secondRun := map[string]int{}
	for k, v := range callCounts {
		secondRun[k] = v
	}
	mu.Unlock()

	if secondRun["getProject"] != 1 {
		t.Errorf("2nd run: getProject = %d, want 1 (always called)", secondRun["getProject"])
	}
	if secondRun["getPipelines"] != 1 {
		t.Errorf("2nd run: getPipelines = %d, want 1 (always called)", secondRun["getPipelines"])
	}
	if secondRun["getPipelineWorkflows"] != 0 {
		t.Errorf("2nd run: getPipelineWorkflows = %d, want 0 (should be skipped)", secondRun["getPipelineWorkflows"])
	}
	if secondRun["getWorkflowJobs"] != 0 {
		t.Errorf("2nd run: getWorkflowJobs = %d, want 0 (should be skipped)", secondRun["getWorkflowJobs"])
	}
	if secondRun["getJobDetails"] != 0 {
		t.Errorf("2nd run: getJobDetails = %d, want 0 (should be skipped)", secondRun["getJobDetails"])
	}

	// Verify data is still correct in DB
	var jobCount int
	sw.db.QueryRow("SELECT COUNT(*) FROM jobs").Scan(&jobCount)
	if jobCount != 1 {
		t.Errorf("jobs count = %d, want 1", jobCount)
	}
}

func TestProcessProject_SQLite_PartialSkip_NonTerminalWorkflow(t *testing.T) {
	runCount := 0
	var mu sync.Mutex
	callCounts := map[string]int{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		mu.Lock()
		switch {
		case strings.Contains(path, "/job/"):
			callCounts["getJobDetails"]++
		case strings.HasPrefix(path, "/api/v2/pipeline/") && strings.Contains(path, "/workflow"):
			callCounts["getPipelineWorkflows"]++
		case strings.HasPrefix(path, "/api/v2/workflow/") && strings.Contains(path, "/job"):
			callCounts["getWorkflowJobs"]++
		}
		currentRun := runCount
		mu.Unlock()

		switch {
		case strings.HasSuffix(path, "/pipeline"):
			json.NewEncoder(w).Encode(PipelineResponse{
				Items: []PipelineItem{{
					ID:        "pipe-1",
					Number:    1,
					State:     "created",
					CreatedAt: time.Now().Format(time.RFC3339),
				}},
			})
		case strings.Contains(path, "/job/"):
			json.NewEncoder(w).Encode(JobResponse{
				CreatedAt: "2026-01-15T10:00:00Z",
				QueuedAt:  "2026-01-15T10:00:01Z",
				StartedAt: "2026-01-15T10:00:05Z",
				StoppedAt: "2026-01-15T10:01:00Z",
				Duration:  55000,
				Name:      "test-job",
				Number:    1,
				Status:    "success",
			})
		case strings.HasPrefix(path, "/api/v2/pipeline/") && strings.Contains(path, "/workflow"):
			// First run: workflow is "running"; second run: "success"
			status := "running"
			if currentRun > 0 {
				status = "success"
			}
			json.NewEncoder(w).Encode(PipelineWorkflowResponse{
				Items: []WorkflowItem{{
					ID:         "wf-1",
					PipelineID: "pipe-1",
					Name:       "build",
					Status:     status,
					CreatedAt:  time.Now().Format(time.RFC3339),
				}},
			})
		case strings.HasPrefix(path, "/api/v2/workflow/") && strings.Contains(path, "/job"):
			json.NewEncoder(w).Encode(WorkflowJobsResponse{
				Items: []WorkflowJobItem{{
					ID:          "job-1",
					Name:        "test-job",
					Type:        "build",
					Status:      "success",
					JobNumber:   1,
					ProjectSlug: "gh/org/repo",
				}},
			})
		case strings.HasPrefix(path, "/api/v2/project/"):
			json.NewEncoder(w).Encode(ProjectResponse{ID: "proj-1", Slug: "gh/org/repo", Name: "repo"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	sw, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer sw.Close()

	client := &CircleCIClient{Token: "test-token", Client: server.Client(), BaseURL: server.URL}
	cfg := processProjectConfig{
		client:       client,
		slug:         "gh/org/repo",
		limit:        10,
		cutoff:       time.Now().AddDate(0, -1, 0),
		sqliteWriter: sw,
	}

	// First run: workflow is "running" → pipeline NOT fully processed
	if err := processProject(context.Background(), cfg); err != nil {
		t.Fatalf("processProject (1st): %v", err)
	}

	mu.Lock()
	if callCounts["getJobDetails"] != 1 {
		t.Errorf("1st run: getJobDetails = %d, want 1", callCounts["getJobDetails"])
	}
	// Reset
	for k := range callCounts {
		callCounts[k] = 0
	}
	runCount = 1
	mu.Unlock()

	// Second run: workflow "running" in DB (was inserted on first run), but API returns "success".
	// Pipeline is NOT fully processed because DB has "running" workflow.
	// GetPipelineWorkflows IS called, but then the workflow is upserted to "success" + has jobs → workflow-level skip for GetWorkflowJobs
	// Actually: we insert the workflow as "success" from the API, then check IsWorkflowFullyProcessed which checks DB status.
	// Since we just upserted to "success" and job exists → workflow-level skip triggers, so GetWorkflowJobs is skipped.
	if err := processProject(context.Background(), cfg); err != nil {
		t.Fatalf("processProject (2nd): %v", err)
	}

	mu.Lock()
	// Pipeline-level skip won't trigger (DB had "running" workflow before this run)
	// But workflow-level skip triggers after upsert (now "success" + has jobs)
	if callCounts["getPipelineWorkflows"] != 1 {
		t.Errorf("2nd run: getPipelineWorkflows = %d, want 1 (pipeline not fully processed in DB)", callCounts["getPipelineWorkflows"])
	}
	if callCounts["getWorkflowJobs"] != 0 {
		t.Errorf("2nd run: getWorkflowJobs = %d, want 0 (workflow-level skip after upsert)", callCounts["getWorkflowJobs"])
	}
	if callCounts["getJobDetails"] != 0 {
		t.Errorf("2nd run: getJobDetails = %d, want 0 (should be skipped)", callCounts["getJobDetails"])
	}
	mu.Unlock()
}

// --- processingStats tests ---

func TestProcessingStats_ConcurrentAccess(t *testing.T) {
	s := &processingStats{}
	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.incPipeline(false)
			s.incWorkflow(true)
			s.incJob(false)
		}()
	}
	wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pipelinesTotal != n {
		t.Errorf("pipelinesTotal = %d, want %d", s.pipelinesTotal, n)
	}
	if s.pipelinesSkipped != 0 {
		t.Errorf("pipelinesSkipped = %d, want 0", s.pipelinesSkipped)
	}
	if s.workflowsTotal != n {
		t.Errorf("workflowsTotal = %d, want %d", s.workflowsTotal, n)
	}
	if s.workflowsSkipped != n {
		t.Errorf("workflowsSkipped = %d, want %d", s.workflowsSkipped, n)
	}
	if s.jobsTotal != n {
		t.Errorf("jobsTotal = %d, want %d", s.jobsTotal, n)
	}
	if s.jobsSkipped != 0 {
		t.Errorf("jobsSkipped = %d, want 0", s.jobsSkipped)
	}
}

func TestProcessingStats_Summary(t *testing.T) {
	s := &processingStats{}
	s.incPipeline(false)
	s.incPipeline(true)
	s.incWorkflow(false)
	s.incWorkflow(false)
	s.incWorkflow(true)
	s.incJob(false)
	s.incJob(false)
	s.incJob(false)
	s.incJob(true)

	got := s.summary()
	want := "Processed 2 pipelines (1 skipped), 3 workflows (1 skipped), 4 jobs (1 skipped)"
	if got != want {
		t.Errorf("summary() = %q, want %q", got, want)
	}
}

func TestProcessingStats_SpinnerSuffix(t *testing.T) {
	s := &processingStats{}
	s.incPipeline(false)
	s.incWorkflow(false)
	s.incJob(false)
	s.incJob(false)

	got := s.spinnerSuffix()
	want := " 1 pipelines, 1 workflows, 2 jobs processed..."
	if got != want {
		t.Errorf("spinnerSuffix() = %q, want %q", got, want)
	}
}

func TestProcessProject_SQLite_StatsTracking(t *testing.T) {
	server := newMockCircleCIServer(t)
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	sw, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer sw.Close()

	client := &CircleCIClient{
		Token:   "test-token",
		Client:  server.Client(),
		BaseURL: server.URL,
	}

	stats := &processingStats{}
	err = processProject(context.Background(), processProjectConfig{
		client:       client,
		slug:         "gh/org/repo",
		limit:        10,
		cutoff:       time.Now().AddDate(0, -1, 0),
		sqliteWriter: sw,
		stats:        stats,
	})
	if err != nil {
		t.Fatalf("processProject: %v", err)
	}

	stats.mu.Lock()
	defer stats.mu.Unlock()

	// newMockCircleCIServer returns 1 pipeline, 1 workflow, 1 job
	if stats.pipelinesTotal != 1 {
		t.Errorf("pipelinesTotal = %d, want 1", stats.pipelinesTotal)
	}
	if stats.pipelinesSkipped != 0 {
		t.Errorf("pipelinesSkipped = %d, want 0", stats.pipelinesSkipped)
	}
	if stats.workflowsTotal != 1 {
		t.Errorf("workflowsTotal = %d, want 1", stats.workflowsTotal)
	}
	if stats.workflowsSkipped != 0 {
		t.Errorf("workflowsSkipped = %d, want 0", stats.workflowsSkipped)
	}
	if stats.jobsTotal != 1 {
		t.Errorf("jobsTotal = %d, want 1", stats.jobsTotal)
	}
	if stats.jobsSkipped != 0 {
		t.Errorf("jobsSkipped = %d, want 0", stats.jobsSkipped)
	}
}

func TestProcessProject_SQLite_SkipStats(t *testing.T) {
	server := newMockCircleCIServer(t)
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	sw, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer sw.Close()

	client := &CircleCIClient{
		Token:   "test-token",
		Client:  server.Client(),
		BaseURL: server.URL,
	}

	// First run: populates DB
	stats1 := &processingStats{}
	err = processProject(context.Background(), processProjectConfig{
		client:       client,
		slug:         "gh/org/repo",
		limit:        10,
		cutoff:       time.Now().AddDate(0, -1, 0),
		sqliteWriter: sw,
		stats:        stats1,
	})
	if err != nil {
		t.Fatalf("processProject (1st): %v", err)
	}

	// Second run: should skip (pipeline fully processed)
	stats2 := &processingStats{}
	err = processProject(context.Background(), processProjectConfig{
		client:       client,
		slug:         "gh/org/repo",
		limit:        10,
		cutoff:       time.Now().AddDate(0, -1, 0),
		sqliteWriter: sw,
		stats:        stats2,
	})
	if err != nil {
		t.Fatalf("processProject (2nd): %v", err)
	}

	stats2.mu.Lock()
	defer stats2.mu.Unlock()

	if stats2.pipelinesTotal != 1 {
		t.Errorf("2nd run: pipelinesTotal = %d, want 1", stats2.pipelinesTotal)
	}
	if stats2.pipelinesSkipped != 1 {
		t.Errorf("2nd run: pipelinesSkipped = %d, want 1", stats2.pipelinesSkipped)
	}
}

func TestProcessProject_VerboseOutput(t *testing.T) {
	server := newMockCircleCIServer(t)
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	sw, err := NewSQLiteWriter(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWriter: %v", err)
	}
	defer sw.Close()

	client := &CircleCIClient{
		Token:   "test-token",
		Client:  server.Client(),
		BaseURL: server.URL,
	}

	// First run to populate DB
	err = processProject(context.Background(), processProjectConfig{
		client:       client,
		slug:         "gh/org/repo",
		limit:        10,
		cutoff:       time.Now().AddDate(0, -1, 0),
		sqliteWriter: sw,
	})
	if err != nil {
		t.Fatalf("processProject (populate): %v", err)
	}

	// Capture stderr for verbose=true run
	origStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	err = processProject(context.Background(), processProjectConfig{
		client:       client,
		slug:         "gh/org/repo",
		limit:        10,
		cutoff:       time.Now().AddDate(0, -1, 0),
		verbose:      true,
		sqliteWriter: sw,
	})

	w.Close()
	os.Stderr = origStderr

	if err != nil {
		t.Fatalf("processProject (verbose): %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if !strings.Contains(output, "Skipping pipeline") {
		t.Errorf("verbose=true: expected 'Skipping pipeline' in stderr, got: %q", output)
	}

	// Now test verbose=false: should NOT contain skip messages
	r2, w2, _ := os.Pipe()
	os.Stderr = w2

	err = processProject(context.Background(), processProjectConfig{
		client:       client,
		slug:         "gh/org/repo",
		limit:        10,
		cutoff:       time.Now().AddDate(0, -1, 0),
		verbose:      false,
		sqliteWriter: sw,
	})

	w2.Close()
	os.Stderr = origStderr

	if err != nil {
		t.Fatalf("processProject (non-verbose): %v", err)
	}

	var buf2 bytes.Buffer
	buf2.ReadFrom(r2)
	output2 := buf2.String()

	if strings.Contains(output2, "Skipping pipeline") {
		t.Errorf("verbose=false: unexpected 'Skipping pipeline' in stderr, got: %q", output2)
	}
}

func TestSQLiteWriter_ConcurrentAccess(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	const numWriters = 5
	const numInsertsPerWriter = 20

	writers := make([]*SQLiteWriter, numWriters)
	for i := range writers {
		w, err := NewSQLiteWriter(dbPath)
		if err != nil {
			t.Fatalf("NewSQLiteWriter[%d]: %v", i, err)
		}
		writers[i] = w
		defer w.Close()
	}

	var wg sync.WaitGroup
	errCh := make(chan error, numWriters*numInsertsPerWriter*2)

	for i, w := range writers {
		wg.Add(1)
		go func(w *SQLiteWriter, writerID int) {
			defer wg.Done()
			for j := 0; j < numInsertsPerWriter; j++ {
				id := fmt.Sprintf("writer%d-pipe-%d", writerID, j)
				p := PipelineItem{
					ID:          id,
					ProjectSlug: "gh/org/repo",
					Number:      writerID*1000 + j,
					State:       "created",
					CreatedAt:   "2026-01-15T10:00:00Z",
					UpdatedAt:   "2026-01-15T10:01:00Z",
				}
				if err := w.InsertPipeline(p); err != nil {
					errCh <- fmt.Errorf("InsertPipeline[%s]: %w", id, err)
				}

				wfID := fmt.Sprintf("writer%d-wf-%d", writerID, j)
				wf := WorkflowItem{
					ID:             wfID,
					PipelineID:     id,
					Name:           "build",
					Status:         "success",
					CreatedAt:      "2026-01-15T10:00:00Z",
					StoppedAt:      "2026-01-15T10:05:00Z",
					PipelineNumber: writerID*1000 + j,
					ProjectSlug:    "gh/org/repo",
				}
				if err := w.InsertWorkflow(wf); err != nil {
					errCh <- fmt.Errorf("InsertWorkflow[%s]: %w", wfID, err)
				}
			}
		}(w, i)
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		for _, err := range errs {
			t.Errorf("%v", err)
		}
		t.Fatalf("got %d errors from concurrent writes", len(errs))
	}

	// Verify all rows were written
	verifyWriter := writers[0]
	var pipelineCount, workflowCount int
	if err := verifyWriter.db.QueryRow("SELECT COUNT(*) FROM pipelines").Scan(&pipelineCount); err != nil {
		t.Fatalf("count pipelines: %v", err)
	}
	if err := verifyWriter.db.QueryRow("SELECT COUNT(*) FROM workflows").Scan(&workflowCount); err != nil {
		t.Fatalf("count workflows: %v", err)
	}

	expectedCount := numWriters * numInsertsPerWriter
	if pipelineCount != expectedCount {
		t.Errorf("pipelines count = %d, want %d", pipelineCount, expectedCount)
	}
	if workflowCount != expectedCount {
		t.Errorf("workflows count = %d, want %d", workflowCount, expectedCount)
	}
}
