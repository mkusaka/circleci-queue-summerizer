package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	circleciapi "github.com/mkusaka/circleci-queue-summerizer/internal/circleciapi"
	"github.com/mkusaka/openapigo"
)

type discardRetryLogger struct{}

func (discardRetryLogger) Printf(string, ...interface{}) {}

func (c *CircleCIClient) retryCheck(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return false, err
	}

	retry, checkErr := retryablehttp.ErrorPropagatedRetryPolicy(ctx, resp, err)
	if retry && c.Warnf != nil {
		c.Warnf("%s; retrying\n", retryMessage(resp, err))
	}
	return retry, checkErr
}

func retryMessage(resp *http.Response, err error) string {
	if err != nil {
		return fmt.Sprintf("Request failed: %v", err)
	}
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		return "Rate limited"
	}
	if resp != nil {
		return fmt.Sprintf("Server error (%d)", resp.StatusCode)
	}
	return "Request failed"
}

func (c *CircleCIClient) generatedClient() *openapigo.Client {
	httpClient := c.retryingHTTPClient()
	opts := []openapigo.Option{
		openapigo.WithBaseURL(strings.TrimRight(c.baseURL(), "/") + "/api/v2"),
		openapigo.WithHTTPClient(httpClient),
	}
	if c.Token != "" {
		opts = append(opts, circleciapi.WithAPIKeyHeaderAuth(c.Token))
	}
	return openapigo.NewClient(opts...)
}

func (c *CircleCIClient) retryingHTTPClient() *http.Client {
	baseClient := c.Client
	if baseClient == nil {
		baseClient = http.DefaultClient
	}

	transportClient := *baseClient
	if transportClient.Transport == nil {
		transportClient.Transport = http.DefaultTransport
	}

	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = maxRetries
	retryClient.RetryWaitMin = initialBackoff
	retryClient.RetryWaitMax = maxBackoff
	retryClient.Backoff = retryablehttp.DefaultBackoff
	retryClient.CheckRetry = c.retryCheck
	retryClient.ErrorHandler = retryablehttp.PassthroughErrorHandler
	retryClient.HTTPClient = &transportClient
	retryClient.Logger = discardRetryLogger{}

	cloned := *baseClient
	cloned.Transport = &retryablehttp.RoundTripper{Client: retryClient}
	return &cloned
}

func (c *CircleCIClient) GetJobDetails(ctx context.Context, projectSlug string, jobNumber int) (*JobResponse, error) {
	slug, err := parseProjectSlug(projectSlug)
	if err != nil {
		return nil, err
	}
	resp, err := openapigo.Do(ctx, c.generatedClient(), circleciapi.GetJobDetails, circleciapi.GetJobDetailsParams{
		Provider:     slug.Provider,
		Organization: slug.Organization,
		Project:      slug.Project,
		JobNumber:    jobNumber,
	})
	if err != nil {
		return nil, err
	}
	return mapJobDetailsResponse(resp), nil
}

func (c *CircleCIClient) GetWorkflowJobs(ctx context.Context, workflowID string, pageToken string) (*WorkflowJobsResponse, error) {
	req := circleciapi.ListWorkflowJobsParams{ID: workflowID}
	if pageToken != "" {
		req.PageToken = &pageToken
	}

	resp, err := openapigo.Do(ctx, c.generatedClient(), circleciapi.ListWorkflowJobs, req)
	if err != nil {
		return nil, err
	}
	return mapWorkflowJobsResponse(resp), nil
}

func (c *CircleCIClient) GetPipelines(ctx context.Context, projectSlug string, pageToken string) (*PipelineResponse, error) {
	slug, err := parseProjectSlug(projectSlug)
	if err != nil {
		return nil, err
	}
	req := circleciapi.ListPipelinesForProjectParams{
		Provider:     slug.Provider,
		Organization: slug.Organization,
		Project:      slug.Project,
	}
	if pageToken != "" {
		req.PageToken = &pageToken
	}

	resp, err := openapigo.Do(ctx, c.generatedClient(), circleciapi.ListPipelinesForProject, req)
	if err != nil {
		return nil, err
	}
	return mapPipelinesResponse(resp), nil
}

func (c *CircleCIClient) GetPipelineWorkflows(ctx context.Context, pipelineID string, pageToken string) (*PipelineWorkflowResponse, error) {
	req := circleciapi.ListWorkflowsByPipelineIDParams{PipelineID: pipelineID}
	if pageToken != "" {
		req.PageToken = &pageToken
	}

	resp, err := openapigo.Do(ctx, c.generatedClient(), circleciapi.ListWorkflowsByPipelineID, req)
	if err != nil {
		return nil, err
	}
	return mapPipelineWorkflowsResponse(resp), nil
}

func (c *CircleCIClient) GetProject(ctx context.Context, projectSlug string) (*ProjectResponse, error) {
	slug, err := parseProjectSlug(projectSlug)
	if err != nil {
		return nil, err
	}
	resp, err := openapigo.Do(ctx, c.generatedClient(), circleciapi.GetProjectBySlug, circleciapi.GetProjectBySlugParams{
		Provider:     slug.Provider,
		Organization: slug.Organization,
		Project:      slug.Project,
	})
	if err != nil {
		return nil, err
	}
	return mapProjectResponse(resp), nil
}

func (c *CircleCIClient) GetOrgProjects(ctx context.Context, orgSlug string) ([]string, error) {
	slug, err := parseOrgSlug(orgSlug)
	if err != nil {
		return nil, err
	}
	resp, err := openapigo.Do(ctx, c.generatedClient(), circleciapi.GetOrgSummaryData, circleciapi.GetOrgSummaryDataParams{
		Provider:     slug.Provider,
		Organization: slug.Organization,
	})
	if err != nil {
		return nil, err
	}
	return append([]string(nil), resp.AllProjects...), nil
}

func mapPipelinesResponse(resp *circleciapi.ListPipelinesForProjectResponse) *PipelineResponse {
	items := make([]PipelineItem, 0, len(resp.Items))
	for _, item := range resp.Items {
		items = append(items, mapPipelineItem(item))
	}
	return &PipelineResponse{
		Items:         items,
		NextPageToken: resp.NextPageToken,
	}
}

func mapPipelineItem(item circleciapi.ListPipelinesForProjectResponseItemsItem) PipelineItem {
	out := PipelineItem{
		ID:          item.ID,
		ProjectSlug: item.ProjectSlug,
		Number:      int(item.Number),
		State:       item.State,
		CreatedAt:   formatTime(item.CreatedAt),
		UpdatedAt:   formatTimePtr(item.UpdatedAt),
	}
	out.Trigger.Type = item.Trigger.Type
	out.Trigger.ReceivedAt = formatTime(item.Trigger.ReceivedAt)
	out.Trigger.Actor.Login = item.Trigger.Actor.Login
	out.Trigger.Actor.AvatarURL = item.Trigger.Actor.AvatarURL

	if item.Vcs != nil {
		out.Vcs.ProviderName = item.Vcs.ProviderName
		out.Vcs.TargetRepositoryURL = item.Vcs.TargetRepositoryURL
		out.Vcs.OriginRepositoryURL = item.Vcs.OriginRepositoryURL
		out.Vcs.Revision = item.Vcs.Revision
		out.Vcs.Branch = stringValue(item.Vcs.Branch)
		out.Vcs.Tag = stringValue(item.Vcs.Tag)
		out.Vcs.ReviewID = stringValue(item.Vcs.ReviewID)
		out.Vcs.ReviewURL = stringValue(item.Vcs.ReviewURL)
		if item.Vcs.Commit != nil {
			out.Vcs.Commit.Subject = item.Vcs.Commit.Subject
			out.Vcs.Commit.Body = item.Vcs.Commit.Body
		}
	}

	return out
}

func mapPipelineWorkflowsResponse(resp *circleciapi.ListWorkflowsByPipelineIDResponse) *PipelineWorkflowResponse {
	items := make([]WorkflowItem, 0, len(resp.Items))
	for _, item := range resp.Items {
		items = append(items, WorkflowItem{
			ID:              item.ID,
			PipelineID:      item.PipelineID,
			Name:            item.Name,
			Status:          item.Status,
			CreatedAt:       formatTime(item.CreatedAt),
			StoppedAt:       formatTime(item.StoppedAt),
			PipelineNumber:  int(item.PipelineNumber),
			ProjectSlug:     item.ProjectSlug,
			StartedBy:       item.StartedBy,
			CanceledBy:      stringValue(item.CanceledBy),
			ErroredBy:       stringValue(item.ErroredBy),
			Tag:             stringValue(item.Tag),
			AutoRerunNumber: int64Value(item.AutoRerunNumber),
			MaxAutoReruns:   int64Value(item.MaxAutoReruns),
		})
	}
	return &PipelineWorkflowResponse{
		Items:         items,
		NextPageToken: resp.NextPageToken,
	}
}

func mapWorkflowJobsResponse(resp *circleciapi.ListWorkflowJobsResponse) *WorkflowJobsResponse {
	items := make([]WorkflowJobItem, 0, len(resp.Items))
	for _, item := range resp.Items {
		items = append(items, WorkflowJobItem{
			ID:                item.ID,
			Name:              item.Name,
			Type:              item.Type,
			Status:            item.Status,
			JobNumber:         int64Value(item.JobNumber),
			StartedAt:         formatTime(item.StartedAt),
			StoppedAt:         formatTimePtr(item.StoppedAt),
			ProjectSlug:       item.ProjectSlug,
			CanceledBy:        stringValue(item.CanceledBy),
			ApprovedBy:        stringValue(item.ApprovedBy),
			ApprovalRequestID: stringValue(item.ApprovalRequestID),
		})
	}
	return &WorkflowJobsResponse{
		Items:         items,
		NextPageToken: resp.NextPageToken,
	}
}

func mapJobDetailsResponse(resp *circleciapi.GetJobDetailsResponse) *JobResponse {
	out := &JobResponse{
		CreatedAt:   formatTime(resp.CreatedAt),
		QueuedAt:    formatTime(resp.QueuedAt),
		StartedAt:   formatTime(resp.StartedAt),
		StoppedAt:   formatTimePtr(resp.StoppedAt),
		Duration:    int(resp.Duration),
		Name:        resp.Name,
		Number:      int(resp.Number),
		WebURL:      resp.WebURL,
		Parallelism: int(resp.Parallelism),
		Status:      resp.Status,
	}
	out.Project.ID = resp.Project.ID
	out.Project.Slug = resp.Project.Slug
	out.Project.Name = resp.Project.Name
	out.Project.ExternalURL = resp.Project.ExternalURL
	out.Executor.ResourceClass = resp.Executor.ResourceClass
	out.Executor.Type = stringValue(resp.Executor.Type)
	out.Organization.Name = resp.Organization.Name
	out.LatestWorkflow.ID = resp.LatestWorkflow.ID
	out.LatestWorkflow.Name = resp.LatestWorkflow.Name
	out.Pipeline.ID = resp.Pipeline.ID
	return out
}

func mapProjectResponse(resp *circleciapi.GetProjectBySlugResponse) *ProjectResponse {
	out := &ProjectResponse{
		ID:               resp.ID,
		Slug:             resp.Slug,
		Name:             resp.Name,
		OrganizationName: resp.OrganizationName,
		OrganizationSlug: resp.OrganizationSlug,
		OrganizationID:   resp.OrganizationID,
	}
	out.VcsInfo.VcsURL = resp.VcsInfo.VcsURL
	out.VcsInfo.Provider = resp.VcsInfo.Provider
	out.VcsInfo.DefaultBranch = resp.VcsInfo.DefaultBranch
	return out
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func formatTimePtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return formatTime(*t)
}

func stringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func int64Value(v *int64) int {
	if v == nil {
		return 0
	}
	return int(*v)
}

func isJobDetailsNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	var apiErr *openapigo.APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == http.StatusNotFound &&
			strings.Contains(strings.ToLower(string(apiErr.Body)), "job not found")
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "404") && strings.Contains(msg, "job not found")
}

type projectSlugParts struct {
	Provider     string
	Organization string
	Project      string
}

type orgSlugParts struct {
	Provider     string
	Organization string
}

func parseProjectSlug(slug string) (projectSlugParts, error) {
	parts := strings.Split(slug, "/")
	if len(parts) != 3 {
		return projectSlugParts{}, fmt.Errorf("invalid project slug %q: expected provider/organization/project", slug)
	}
	return projectSlugParts{
		Provider:     parts[0],
		Organization: parts[1],
		Project:      parts[2],
	}, nil
}

func parseOrgSlug(slug string) (orgSlugParts, error) {
	parts := strings.Split(slug, "/")
	if len(parts) != 2 {
		return orgSlugParts{}, fmt.Errorf("invalid org slug %q: expected provider/organization", slug)
	}
	return orgSlugParts{
		Provider:     parts[0],
		Organization: parts[1],
	}, nil
}
