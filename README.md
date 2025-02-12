# circleci-queue-summerizer

A CLI tool to analyze queue times of CircleCI jobs.

## Features

- Fetch queue times for recent pipelines in specified projects
- Support multiple projects at once
- Configurable number of pipelines to analyze
- Output in table or JSON format
- Detailed information including workflow IDs, pipeline IDs, and job statuses
- Skip running jobs automatically

## Installation

```
go install github.com/your-username/circleci-queue-summerizer@latest
```

Or build from source:

```
git clone https://github.com/your-username/circleci-queue-summerizer
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

# Get queue times for the last 30 pipelines
circleci-queue-summerizer -p gh/org/repo1 -p gh/org/repo2 --limit 30

# Output in JSON format
circleci-queue-summerizer -p gh/org/repo1 -p gh/org/repo2 --format json

# Silent mode (only errors)
circleci-queue-summerizer -p gh/org/repo1 --silent
```

### Options

```
--project, -p     Project slug (required, e.g., gh/org/repo). Can be specified multiple times
--token, -t       CircleCI API token (can also be set via CIRCLECI_TOKEN env var)
--limit           Number of pipelines to fetch per project (default: 10)
--format          Output format (table or json, default: table)
--silent          Suppress all output except errors
```

### Output Example

```
Repository           Workflow    Workflow ID                         Pipeline ID                         Job            Job ID                              Number    Status    Queued At                Started At               Queue Time
---------           --------    -----------                         -----------                         ---            -------                             ------    ------    ---------                ----------               ----------
gh/org/repo1        build       01234567-89ab-cdef-0123-456789abcdef 98765432-10fe-dcba-9876-543210fedcba test           abcdef01-2345-6789-abcd-ef0123456789 123       success   2023-04-01T12:00:00Z    2023-04-01T12:00:05Z    5s
gh/org/repo2        test        fedcba98-7654-3210-fedc-ba9876543210 abcdef01-2345-6789-abcd-ef0123456789 lint           01234567-89ab-cdef-0123-456789abcdef 456       failed    2023-04-01T12:01:00Z    2023-04-01T12:01:10Z    10s
```

## Notes

- Requires a CircleCI API token
- Be mindful of API rate limits
- Project slug should be in `gh/org/repo` format (for GitHub repositories)
- Multiple projects are processed concurrently
- Running jobs are automatically skipped
