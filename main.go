package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
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
	JobID        string        `json:"job_id"`
	Status       string        `json:"status"`
	QueuedAt     time.Time     `json:"queued_at"`
	StartedAt    time.Time     `json:"started_at"`
	QueueTime    time.Duration `json:"queue_time"`
	WorkflowName string        `json:"workflow_name"`
	WorkflowID   string        `json:"workflow_id"`
	PipelineID   string        `json:"pipeline_id"`
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
		Status    string `json:"status"`
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

	var workflows WorkflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&workflows); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}

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

func (c *CircleCIClient) GetPipelines(projectSlug string, pageToken string) (*PipelineResponse, error) {
	baseURL := fmt.Sprintf("https://circleci.com/api/v2/project/%s/pipeline", projectSlug)
	url := baseURL
	if pageToken != "" {
		url = fmt.Sprintf("%s?page-token=%s", baseURL, pageToken)
	}

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

	var pipelines PipelineResponse
	if err := json.NewDecoder(resp.Body).Decode(&pipelines); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}

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

func main() {
	app := &cli.App{
		Name:  "circleci-queue-time",
		Usage: "Get queue times for CircleCI jobs",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:     "project",
				Aliases:  []string{"p"},
				Usage:    "Project slug (e.g. gh/org/repo). Can be specified multiple times",
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
			&cli.IntFlag{
				Name:  "limit",
				Value: 10,
				Usage: "Number of jobs to fetch",
			},
		},
		Action: func(c *cli.Context) error {
			projects := c.StringSlice("project")
			limit := c.Int("limit")

			client := &CircleCIClient{
				Token:  c.String("token"),
				Client: &http.Client{},
			}

			// チャネルを作成
			jobsChan := make(chan JobQueueInfo)
			errChan := make(chan error, len(projects))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// 出力用のWaitGroup
			var wg sync.WaitGroup
			wg.Add(1)

			// 出力処理を開始
			go func() {
				defer wg.Done()
				if c.String("format") == "json" {
					fmt.Println("[")
					first := true
					for job := range jobsChan {
						if !first {
							fmt.Println(",")
						}
						json.NewEncoder(os.Stdout).Encode(job)
						first = false
					}
					fmt.Println("]")
				} else {
					w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.TabIndent)
					fmt.Fprintln(w, "Repository\tWorkflow\tWorkflow ID\tPipeline ID\tJob\tJob ID\tNumber\tStatus\tQueued At\tStarted At\tQueue Time")
					fmt.Fprintln(w, "---------\t--------\t-----------\t-----------\t---\t-------\t------\t------\t---------\t----------\t----------")
					w.Flush()

					for job := range jobsChan {
						fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\n",
							job.Repository,
							job.WorkflowName,
							job.WorkflowID,
							job.PipelineID,
							job.JobName,
							job.JobID,
							job.JobNumber,
							job.Status,
							job.QueuedAt.Format(time.RFC3339),
							job.StartedAt.Format(time.RFC3339),
							job.QueueTime,
						)
						w.Flush()
					}
				}
			}()

			// プロジェクトごとの処理用WaitGroup
			var projectWg sync.WaitGroup
			projectWg.Add(len(projects))

			// 各プロジェクトを並列処理
			for _, projectSlug := range projects {
				go func(slug string) {
					defer projectWg.Done()
					slug = strings.Replace(slug, "github/", "gh/", 1)

					count := 0
					var nextPageToken string

					// パイプラインの取得とジョブ情報の収集
					for {
						pipelines, err := client.GetPipelines(slug, nextPageToken)
						if err != nil {
							errChan <- fmt.Errorf("\n❌ Error in %s:\n   %v", slug, err)
							return
						}

						for _, pipeline := range pipelines.Items {
							if count >= limit {
								break
							}

							workflows, err := client.GetPipelineWorkflows(pipeline.ID)
							if err != nil {
								fmt.Printf("Error getting workflows for pipeline %s: %v\n", pipeline.ID, err)
								continue
							}

							hasProcessedPipeline := false

							for _, workflow := range workflows.Items {
								jobs, err := client.GetWorkflowJobs(workflow.ID)
								if err != nil {
									fmt.Printf("\n⚠️  Workflow %s (%s):\n   %v\n", workflow.Name, workflow.ID, err)
									continue
								}

								for _, job := range jobs.Items {
									if job.JobNumber == 0 {
										fmt.Fprintf(os.Stderr, "Skipping job with number 0 (Workflow: https://circleci.com/workflow-run/%s)\n",
											workflow.ID)
										continue
									}

									jobDetails, err := client.GetJobDetails(slug, job.JobNumber)
									if err != nil {
										fmt.Fprintf(os.Stderr, "\n⚠️  Job %d in workflow %s:\n   %v\n",
											job.JobNumber, workflow.Name, err)
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
										JobID:        job.ID,
										Status:       job.Status,
										QueuedAt:     queuedAt,
										StartedAt:    startedAt,
										QueueTime:    startedAt.Sub(queuedAt),
										WorkflowName: workflow.Name,
										WorkflowID:   workflow.ID,
										PipelineID:   pipeline.ID,
									}

									select {
									case <-ctx.Done():
										return
									case jobsChan <- info:
										hasProcessedPipeline = true
									}
								}
							}

							if hasProcessedPipeline {
								count++
							}
						}

						if count >= limit {
							break
						}

						nextPageToken = pipelines.NextPageToken
						if nextPageToken == "" {
							break
						}
					}
				}(projectSlug)
			}

			// すべてのプロジェクトの処理完了を待つ
			projectWg.Wait()
			close(jobsChan)

			// エラーチェック
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

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
