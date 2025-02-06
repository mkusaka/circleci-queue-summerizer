# circleci-queue-summerizer

A CLI tool to analyze queue times of CircleCI jobs.

## Features

- Fetch queue times for recent pipelines in a specified project
- Configurable number of pipelines to analyze
- Output in table or JSON format
- Detailed information including workflow IDs and pipeline IDs

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

# Get queue times for the last 10 pipelines (default)
circleci-queue-summerizer -p gh/org/repo

# Get queue times for the last 30 pipelines
circleci-queue-summerizer -p gh/org/repo --limit 30

# Output in JSON format
circleci-queue-summerizer -p gh/org/repo --format json
```

### Options

```
--project, -p     Project slug (required, e.g., gh/org/repo)
--token, -t       CircleCI API token (can also be set via CIRCLECI_TOKEN env var)
--limit           Number of pipelines to fetch (default: 10)
--format          Output format (table or json, default: table)
```

### Output Example

```
Repository           Workflow    Workflow ID                         Pipeline ID                         Job            Number    Queued At                Started At               Queue Time
---------           --------    -----------                         -----------                         ---            ------    ---------                ----------               ----------
gh/org/repo         build       01234567-89ab-cdef-0123-456789abcdef 98765432-10fe-dcba-9876-543210fedcba test           123       2023-04-01T12:00:00Z    2023-04-01T12:00:05Z    5s
```

## Notes

- Requires a CircleCI API token
- Be mindful of API rate limits
- Project slug should be in `gh/org/repo` format (for GitHub repositories)
