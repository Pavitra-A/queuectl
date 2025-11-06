from typing import Dict, Any

class JobExecutionError(Exception):
    pass

def print_message_handler(payload: Dict[str, Any]) -> None:
    """
    Example handler that prints a message.
    If payload contains {"fail": true}, it raises an error.
    """
    if payload.get("fail"):
        raise JobExecutionError("Simulated failure in print_message_handler")

    msg = payload.get("msg", "<no message>")
    print(f"[print_message] {msg}")

HANDLERS = {
    "print_message": print_message_handler,
}
