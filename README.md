# **QueueCTL – CLI-Based Background Job Queue System**

A **CLI-based background job queue system** built using **Python**, **Typer**, and **SQLite**.  
This project demonstrates backend development fundamentals such as background job scheduling, retries with exponential backoff, and Dead Letter Queue (DLQ) management.

---

## **Overview**

`QueueCTL` is a **minimal, production-inspired job queue system** that runs from the command line.  
It simulates how real-world background processing frameworks (like Celery or BullMQ) handle asynchronous workloads.

The system allows you to:

- Enqueue jobs with specific types and payloads  
- Run worker processes that execute jobs in the background  
- Automatically retry failed jobs using an **exponential backoff** strategy  
- Move permanently failed jobs into a **Dead Letter Queue (DLQ)**  
- Inspect and requeue DLQ jobs through a simple CLI  

This project focuses on **simplicity, reliability, and extensibility** — as a foundation for building scalable background job systems.

---

## **Python File Breakdown**

| File | Purpose |
|------|--------|
| **`queuectl/cli.py`** | Entry point for all **CLI commands**. Handles initialization (`init`), enqueueing new jobs (`enqueue`), running workers (`worker`), listing jobs, and managing the DLQ (`dlq list`, `dlq retry`). |
| **`queuectl/db.py`** | Manages the **SQLite database connection** and creates required tables (`jobs`, `dlq`) for persistent job storage. |
| **`queuectl/job_store.py`** | Contains the **core logic** for job management — enqueueing, retrieving pending jobs, retry handling, updating statuses, and moving failed jobs into the DLQ. |
| **`queuectl/worker.py`** | Implements the **background worker** that continuously polls for pending jobs, executes them, logs their progress, retries failed jobs, and applies exponential backoff. |
| **`queuectl/handlers.py`** | Defines the **job handler functions**. For instance, a `print_message_handler` to simulate background processing tasks. |
| **`requirements.txt`** | Lists the external dependency (`typer`). |
| **`README.md`** | Project documentation describing design, setup, and features. |

---

## **Database Design**

QueueCTL uses **SQLite** as its lightweight persistent storage.  
Two main tables are maintained to manage the job lifecycle.

### 1. `jobs`

Stores all queued jobs and their states.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Unique job ID |
| `type` | TEXT | Job type (handler name) |
| `payload` | TEXT | JSON data passed to handler |
| `status` | TEXT | Job status: `pending`, `running`, `completed`, `failed`, or `dlq` |
| `attempts` | INTEGER | Number of attempts made so far |
| `max_attempts` | INTEGER | Maximum retries before moving to DLQ |
| `available_at` | DATETIME | Time when job becomes available |

### 2. `dlq` (Dead Letter Queue)

Stores permanently failed jobs after all retries have been exhausted.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | DLQ entry ID |
| `original_job_id` | INTEGER | Reference to the original job in `jobs` table |
| `error_message` | TEXT | Reason for failure |
| `payload` | TEXT | Job data that failed |
| `created_at` | DATETIME | Timestamp when moved to DLQ |

---

## **Usage**

Follow these steps to run and test the QueueCTL system:

```bash
# 1. Initialize the database
python -m queuectl.cli init

# 2. Enqueue a new job
python -m queuectl.cli enqueue --type print_message --payload '{"msg": "Hello from QueueCTL!"}'

# 3. Start the worker to process jobs
python -m queuectl.cli worker

# 4. List all jobs and their statuses
python -m queuectl.cli list

# 5. View jobs in the Dead Letter Queue (DLQ)
python -m queuectl.cli dlq list

# 6. Retry a specific DLQ job
python -m queuectl.cli dlq retry <job_id>
