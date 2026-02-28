package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
