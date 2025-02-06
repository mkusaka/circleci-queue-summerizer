package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/urfave/cli/v2"
)

type CircleCIClient struct {
	Token  string
	Client *http.Client
}

type JobQueueInfo struct {
	Repository   string        `json:"repository"`
	JobName      string        `json:"job_name"`
	JobNumber    int           `json:"job_number"`
	QueuedAt     time.Time     `json:"queued_at"`
	StartedAt    time.Time     `json:"started_at"`
	QueueTime    time.Duration `json:"queue_time"`
	WorkflowName string        `json:"workflow_name"`
}

type JobResponse struct {
	QueuedAt  string `json:"queued_at"`
	StartedAt string `json:"started_at"`
	Name      string `json:"name"`
	Number    int    `json:"number"`
	Project   struct {
		Slug string `json:"slug"`
	} `json:"project"`
}

type WorkflowResponse struct {
	Items []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"items"`
	NextPageToken string `json:"next_page_token"`
}

type WorkflowJobsResponse struct {
	Items []struct {
		JobNumber int    `json:"job_number"`
		ID        string `json:"id"`
	} `json:"items"`
	NextPageToken string `json:"next_page_token"`
}

type PipelineResponse struct {
	Items []struct {
		ID        string `json:"id"`
		State     string `json:"state"`
		Number    int    `json:"number"`
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
	} `json:"items"`
	NextPageToken string `json:"next_page_token"`
}

type PipelineWorkflowResponse struct {
	Items []struct {
		PipelineID string `json:"pipeline_id"`
		ID         string `json:"id"`
		Name       string `json:"name"`
		Status     string `json:"status"`
		CreatedAt  string `json:"created_at"`
	} `json:"items"`
}

func (c *CircleCIClient) GetJobDetails(projectSlug string, jobNumber int) (*JobResponse, error) {
	url := fmt.Sprintf("https://circleci.com/api/v2/project/%s/job/%d", projectSlug, jobNumber)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Circle-Token", c.Token)
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API error: %s", resp.Status)
	}

	var job JobResponse
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}

	return &job, nil
}

func (c *CircleCIClient) GetWorkflows(projectSlug string) (*WorkflowResponse, error) {
	url := fmt.Sprintf("https://circleci.com/api/v2/insights/%s/workflows/summary", projectSlug)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Circle-Token", c.Token)
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	fmt.Printf("Workflows API Status: %s\n", resp.Status)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: %s - %s", resp.Status, string(body))
	}

	var workflows WorkflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&workflows); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}

	fmt.Printf("Found %d workflows\n", len(workflows.Items))

	return &workflows, nil
}

func (c *CircleCIClient) GetWorkflowJobs(workflowID string) (*WorkflowJobsResponse, error) {
	url := fmt.Sprintf("https://circleci.com/api/v2/workflow/%s/job", workflowID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Circle-Token", c.Token)
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API error: %s", resp.Status)
	}

	var jobs WorkflowJobsResponse
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		return nil, err
	}

	return &jobs, nil
}

func (c *CircleCIClient) GetPipelines(projectSlug string) (*PipelineResponse, error) {
	url := fmt.Sprintf("https://circleci.com/api/v2/project/%s/pipeline", projectSlug)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Circle-Token", c.Token)
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	fmt.Printf("Pipelines API Status: %s\n", resp.Status)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: %s - %s", resp.Status, string(body))
	}

	var pipelines PipelineResponse
	if err := json.NewDecoder(resp.Body).Decode(&pipelines); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}

	fmt.Printf("Found %d pipelines\n", len(pipelines.Items))
	return &pipelines, nil
}

func (c *CircleCIClient) GetPipelineWorkflows(pipelineID string) (*PipelineWorkflowResponse, error) {
	url := fmt.Sprintf("https://circleci.com/api/v2/pipeline/%s/workflow", pipelineID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Circle-Token", c.Token)
	resp, err := c.Client.Do(req)
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

func printTable(jobs []JobQueueInfo) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.TabIndent)
	fmt.Fprintln(w, "Repository\tWorkflow\tJob\tNumber\tQueued At\tStarted At\tQueue Time")
	fmt.Fprintln(w, "---------\t--------\t---\t------\t---------\t----------\t----------")

	for _, job := range jobs {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
			job.Repository,
			job.WorkflowName,
			job.JobName,
			job.JobNumber,
			job.QueuedAt.Format(time.RFC3339),
			job.StartedAt.Format(time.RFC3339),
			job.QueueTime,
		)
	}
	w.Flush()
}

func main() {
	app := &cli.App{
		Name:  "circleci-queue-time",
		Usage: "Get queue times for CircleCI jobs",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "project",
				Aliases:  []string{"p"},
				Usage:    "Project slug (e.g. gh/org/repo)",
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
				Usage: "Output format (table, json)",
			},
		},
		Action: func(c *cli.Context) error {
			projectSlug := strings.Replace(c.String("project"), "github/", "gh/", 1)

			client := &CircleCIClient{
				Token:  c.String("token"),
				Client: &http.Client{},
			}

			fmt.Printf("Fetching pipelines for project: %s\n", projectSlug)
			pipelines, err := client.GetPipelines(projectSlug)
			if err != nil {
				return err
			}

			var allJobs []JobQueueInfo
			for _, pipeline := range pipelines.Items {
				workflows, err := client.GetPipelineWorkflows(pipeline.ID)
				if err != nil {
					fmt.Printf("Error getting workflows for pipeline %s: %v\n", pipeline.ID, err)
					continue
				}

				for _, workflow := range workflows.Items {
					jobs, err := client.GetWorkflowJobs(workflow.ID)
					if err != nil {
						fmt.Printf("Error getting jobs for workflow %s: %v\n", workflow.ID, err)
						continue
					}

					for _, job := range jobs.Items {
						jobDetails, err := client.GetJobDetails(projectSlug, job.JobNumber)
						if err != nil {
							fmt.Printf("Error getting details for job %d: %v\n", job.JobNumber, err)
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

						info := JobQueueInfo{
							Repository:   jobDetails.Project.Slug,
							JobName:      jobDetails.Name,
							JobNumber:    jobDetails.Number,
							QueuedAt:     queuedAt,
							StartedAt:    startedAt,
							QueueTime:    startedAt.Sub(queuedAt),
							WorkflowName: workflow.Name,
						}
						allJobs = append(allJobs, info)
					}
				}
			}

			if c.String("format") == "json" {
				json.NewEncoder(os.Stdout).Encode(allJobs)
			} else {
				printTable(allJobs)
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
