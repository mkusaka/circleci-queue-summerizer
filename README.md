# circleci-queue-summerizer

A CLI tool to analyze queue times of CircleCI jobs.

## Features

- Fetch queue times for recent pipelines in specified projects
- Support multiple projects at once
- Auto-discover all projects in an organization with `-p all:{org-slug}`
- Configurable number of pipelines to analyze
- Time-based filtering with `--months` flag
- Output in table, NDJSON, or SQLite format
- SQLite output with upsert support (`INSERT OR REPLACE`) for incremental data collection
- Automatic retry with exponential backoff for API rate limits (429) and server errors (5xx)
- Detailed information including workflow IDs, pipeline IDs, and job statuses

## Installation

```
go install github.com/mkusaka/circleci-queue-summerizer@latest
```

Or build from source:

```
git clone https://github.com/mkusaka/circleci-queue-summerizer
cd circleci-queue-summerizer
go build
```

## Usage

### Basic Usage

```
# Set your CircleCI token
export CIRCLECI_TOKEN=your-circleci-token

# Get queue times for a single project
circleci-queue-summerizer -p gh/org/repo1

# Get queue times for multiple projects
circleci-queue-summerizer -p gh/org/repo1 -p gh/org/repo2 -p gh/org/repo3

# Get queue times for all projects in an organization
circleci-queue-summerizer -p all:gh/org

# Get queue times for the last 30 pipelines
circleci-queue-summerizer -p gh/org/repo1 --limit 30

# Get queue times for the last 3 months
circleci-queue-summerizer -p gh/org/repo1 --months 3

# Output in NDJSON format
circleci-queue-summerizer -p gh/org/repo1 --format ndjson

# Output to SQLite database
circleci-queue-summerizer -p gh/org/repo1 --format sqlite -o queue_times.db

# Verbose mode (show detailed progress)
circleci-queue-summerizer -p gh/org/repo1 --verbose
```

### Options

```
--project, -p     Project slug (required, e.g., gh/org/repo). Can be specified multiple times.
                  Use all:{org-slug} to auto-discover all projects in an organization
--token, -t       CircleCI API token (can also be set via CIRCLECI_TOKEN env var)
--format          Output format: table, ndjson, or sqlite (default: table)
--output, -o      Output file path (required for sqlite format)
--limit           Number of pipelines to fetch per project (default: 10)
--months          Number of months to look back (default: 1)
--verbose, -v     Show detailed progress messages on stderr
```

### SQLite Output

When using `--format sqlite`, data is stored in 4 tables:

- **projects** — project metadata (slug, name, organization, VCS info)
- **pipelines** — pipeline runs (state, trigger info, VCS revision/branch)
- **workflows** — workflow executions per pipeline
- **jobs** — individual job details (queue time, duration, executor, status)

Re-running with the same `-o` path performs upserts — existing rows are updated, no duplicates are created.

```
# Collect data
circleci-queue-summerizer -p gh/org/repo --format sqlite -o data.db --months 3

# Query with sqlite3
sqlite3 data.db "SELECT name, AVG(queue_time_ms) FROM jobs GROUP BY name"
```

## Notes

- Requires a CircleCI API token
- API rate limits are handled automatically with exponential backoff and `Retry-After` header support
- Project slug should be in `gh/org/repo` format (for GitHub repositories)
- Multiple projects are processed concurrently
- Queue time is measured in milliseconds (`queue_time_ms` in SQLite, `queue_time` in NDJSON/table)
