# queuectl

A CLI-based background job queue with worker processes, automatic retries
with exponential backoff, and a Dead Letter Queue (DLQ) for permanently
failed jobs.

Status: work in progress — this README is being filled in as the system is
built. See commit history for progress.

## Requirements

- Python 3.9+ (stdlib only, no external dependencies)
- bash (for the `bin/queuectl` wrapper)

## Setup

```bash
git clone https://github.com/durgaanuhya02/FLAM-COMPANY-ASSIGNMENT.git
cd FLAM-COMPANY-ASSIGNMENT/queuectl
export PATH="$PWD/bin:$PATH"
queuectl --help
```

More usage examples, architecture overview, and testing instructions will
be added as the corresponding functionality lands.
