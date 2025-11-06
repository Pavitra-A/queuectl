import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from .db import get_connection, now_utc_iso


def enqueue_job(
    job_type: str,
    payload: Dict[str, Any],
    max_attempts: int = 5,
    available_at: Optional[datetime] = None,
) -> int:
    """Insert a new job into the queue and return its ID."""
    if available_at is None:
        available_at = datetime.utcnow()

    payload_str = json.dumps(payload)

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO jobs (type, payload, status, attempts, max_attempts,
                              available_at, last_error, created_at, updated_at)
            VALUES (?, ?, 'pending', 0, ?, ?, NULL, ?, ?)
            """,
            (
                job_type,
                payload_str,
                max_attempts,
                available_at.isoformat(),
                now_utc_iso(),
                now_utc_iso(),
            ),
        )
        return cur.lastrowid


def list_jobs(status: Optional[str] = None) -> List[Dict[str, Any]]:
    """List jobs, optionally filtered by status."""
    with get_connection() as conn:
        cur = conn.cursor()
        if status:
            cur.execute("SELECT * FROM jobs WHERE status = ? ORDER BY id DESC", (status,))
        else:
            cur.execute("SELECT * FROM jobs ORDER BY id DESC")
        rows = cur.fetchall()

    return [dict(row) for row in rows]


def get_job(job_id: int) -> Optional[Dict[str, Any]]:
    """Fetch a single job by ID."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def get_next_pending_job() -> Optional[Dict[str, Any]]:
    """
    Fetch the next available pending job and mark it as 'running'.
    Returns the job dict or None if no job is ready.
    """
    now_iso = datetime.utcnow().isoformat()

    with get_connection() as conn:
        cur = conn.cursor()
        # Pick the oldest pending job that is ready
        cur.execute(
            """
            SELECT * FROM jobs
            WHERE status = 'pending'
              AND available_at <= ?
            ORDER BY id
            LIMIT 1
            """,
            (now_iso,),
        )
        row = cur.fetchone()
        if not row:
            return None

        job_id = row["id"]
        # Mark as running
        cur.execute(
            "UPDATE jobs SET status = 'running', updated_at = ? WHERE id = ?",
            (now_utc_iso(), job_id),
        )

    return dict(row)


def mark_job_completed(job_id: int) -> None:
    """Mark job as completed."""
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET status = 'completed',
                updated_at = ?
            WHERE id = ?
            """,
            (now_utc_iso(), job_id),
        )


def move_job_to_dlq(job_id: int, error_message: str) -> None:
    """Move job directly to DLQ with an error message."""
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET status = 'dlq',
                last_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (error_message, now_utc_iso(), job_id),
        )


def handle_job_failure(job: Dict[str, Any], error_message: str, base_delay: int = 5) -> None:
    """
    Handle a job failure with exponential backoff.
    - Increase attempts
    - If attempts < max_attempts: reschedule as pending with backoff
    - Else: move to DLQ
    """
    job_id = job["id"]
    attempts = job["attempts"]
    max_attempts = job["max_attempts"]

    new_attempts = attempts + 1

    if new_attempts >= max_attempts:
        # Move to DLQ
        with get_connection() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = 'dlq',
                    attempts = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (new_attempts, error_message, now_utc_iso(), job_id),
            )
    else:
        # Exponential backoff: delay = base_delay * 2^(new_attempts - 1)
        delay_seconds = base_delay * (2 ** (new_attempts - 1))
        next_available = datetime.utcnow() + timedelta(seconds=delay_seconds)

        with get_connection() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = 'pending',
                    attempts = ?,
                    available_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    new_attempts,
                    next_available.isoformat(),
                    error_message,
                    now_utc_iso(),
                    job_id,
                ),
            )


def retry_dlq_job(job_id: int) -> None:
    """
    Take a DLQ job and requeue it as pending again.
    Here we reset attempts to 0 and clear last_error.
    """
    job = get_job(job_id)
    if job is None:
        raise ValueError(f"Job #{job_id} not found")
    if job["status"] != "dlq":
        raise ValueError(f"Job #{job_id} is not in DLQ (status={job['status']})")

    with get_connection() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET status = 'pending',
                attempts = 0,
                available_at = ?,
                last_error = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (datetime.utcnow().isoformat(), now_utc_iso(), job_id),
        )
