import json
from typing import Optional

import typer

from .db import init_db
from .job_store import (
    enqueue_job,
    list_jobs,
    get_job,
    retry_dlq_job,
)
from .worker import run_worker

app = typer.Typer(help="queuectl - simple background job queue CLI")
dlq_app = typer.Typer(help="Dead Letter Queue (DLQ) management")
app.add_typer(dlq_app, name="dlq")


@app.command()
def init():
    """
    Initialize the queue database (creates queuectl.db and jobs table).
    """
    init_db()
    typer.echo("Database initialized (queuectl.db).")


@app.command()
def enqueue(
    type: str = typer.Option(..., "--type", "-t", help="Job type (e.g., print_message)"),
    payload: str = typer.Option(..., "--payload", "-p", help="JSON payload string"),
    max_attempts: int = typer.Option(
        5, "--max-attempts", "-m", help="Maximum retry attempts for the job"
    ),
):
    """
    Enqueue a new job.
    """
    try:
        payload_dict = json.loads(payload)
    except json.JSONDecodeError as e:
        raise typer.BadParameter(f"Invalid JSON payload: {e}") from e

    job_id = enqueue_job(type, payload_dict, max_attempts=max_attempts)
    typer.echo(f"Enqueued job #{job_id} (type={type}).")


@app.command()
def list(
    status: Optional[str] = typer.Option(
        None,
        "--status",
        "-s",
        help="Filter by status: pending|running|completed|failed|dlq",
    )
):
    """
    List jobs, optionally filtered by status.
    """
    jobs = list_jobs(status=status)
    if not jobs:
        typer.echo("No jobs found.")
        raise typer.Exit(code=0)

    for job in jobs:
        typer.echo(
            f"#{job['id']} "
            f"type={job['type']} "
            f"status={job['status']} "
            f"attempts={job['attempts']}/{job['max_attempts']} "
            f"available_at={job['available_at']}"
        )


@app.command()
def worker(
    poll_interval: float = typer.Option(
        2.0,
        "--poll-interval",
        "-i",
        help="Seconds to wait between polls when no jobs are available.",
    ),
    base_delay: int = typer.Option(
        5,
        "--base-delay",
        "-b",
        help="Base delay (seconds) for exponential backoff on retries.",
    ),
):
    """
    Run a worker process that continuously processes jobs.
    """
    typer.echo(
        f"Starting worker (poll_interval={poll_interval}s, base_delay={base_delay}s). "
        "Press Ctrl+C to stop."
    )
    try:
        run_worker(poll_interval=poll_interval, base_delay=base_delay)
    except KeyboardInterrupt:
        typer.echo("Worker stopped.")


@dlq_app.command("list")
def dlq_list():
    """
    List jobs that are currently in the Dead Letter Queue (DLQ).
    """
    jobs = list_jobs(status="dlq")
    if not jobs:
        typer.echo("No jobs in DLQ.")
        raise typer.Exit(code=0)

    for job in jobs:
        typer.echo(
            f"#{job['id']} type={job['type']} attempts={job['attempts']}/"
            f"{job['max_attempts']} last_error={job.get('last_error')}"
        )


@dlq_app.command("show")
def dlq_show(job_id: int = typer.Argument(..., help="ID of the DLQ job to inspect")):
    """
    Show full details of a single DLQ job.
    """
    job = get_job(job_id)
    if job is None:
        typer.echo(f"Job #{job_id} not found.")
        raise typer.Exit(code=1)

    if job["status"] != "dlq":
        typer.echo(f"Job #{job_id} is not in DLQ (status={job['status']}).")
        raise typer.Exit(code=1)

    typer.echo(json.dumps(job, indent=2))


@dlq_app.command("retry")
def dlq_retry(job_id: int = typer.Argument(..., help="ID of the DLQ job to retry")):
    """
    Requeue a DLQ job back into the main queue as pending.
    """
    try:
        retry_dlq_job(job_id)
    except ValueError as e:
        typer.echo(str(e))
        raise typer.Exit(code=1)

    typer.echo(f"Job #{job_id} requeued from DLQ to pending.")


def main():
    app()


if __name__ == "__main__":
    main()
