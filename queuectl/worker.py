import json
import time
from typing import Any, Dict

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


from . import handlers
from .job_store import (
    get_next_pending_job,
    mark_job_completed,
    handle_job_failure,
    move_job_to_dlq,
)


def run_worker(poll_interval: float = 2.0, base_delay: int = 5) -> None:
    """
    Main worker loop:
    - Poll for pending jobs
    - Execute handlers
    - Handle success/failure with retries and DLQ
    """
    #print(f"[worker] Starting worker (poll_interval={poll_interval}s, base_delay={base_delay}s)")
    logger.info("Starting worker (poll_interval=%ss, base_delay=%ss)", poll_interval, base_delay)

    while True:
        job = get_next_pending_job()

        if job is None:
            # No job ready; sleep and poll again
            time.sleep(poll_interval)
            continue

        job_id = job["id"]
        job_type = job["type"]
        payload_str = job["payload"]

        #print(f"[worker] Picked job #{job_id} (type={job_type})")
        logger.info("Picked job #%s (type=%s)", job_id, job_type)

        # Decode payload
        try:
            payload: Dict[str, Any] = json.loads(payload_str)
        except json.JSONDecodeError:
            #print(f"[worker] Job #{job_id} has invalid JSON payload, moving to DLQ")
            logger.error("Job #%s has invalid JSON payload, moving to DLQ", job_id)
            move_job_to_dlq(job_id, "Invalid JSON payload stored")
            continue

        # Find handler
        handler = handlers.HANDLERS.get(job_type)
        if handler is None:
            #print(f"[worker] No handler for job type '{job_type}', moving to DLQ")
            logger.error("No handler for job type '%s', moving to DLQ", job_type)
            move_job_to_dlq(job_id, f"Unknown job type: {job_type}")
            continue

        # Execute handler
        try:
            #print(f"[worker] Executing job #{job_id}")
            logger.info("Executing job #%s", job_id)
            handler(payload)
        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            #print(f"[worker] Job #{job_id} failed: {error_msg}")
            logger.error("Job #%s failed: %s", job_id, error_msg)
            handle_job_failure(job, error_msg, base_delay=base_delay)
        else:
            #print(f"[worker] Job #{job_id} completed successfully")
            logger.info("Job #%s completed successfully", job_id)
            mark_job_completed(job_id)
