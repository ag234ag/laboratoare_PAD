import oracle
from clients import wait_until


def expected_counts(messages=None, deliveries=None, dlq_delivery=0, dlq_invalid=0):
    return {
        "messages": messages or {},
        "deliveries": deliveries or {},
        "dead_letters_invalid": dlq_invalid,
        "dead_letters_delivery": dlq_delivery,
    }


def wait_for_db_status(db_path, message_id, status, timeout=15):
    wait_until(lambda: oracle.message_status(db_path, message_id) == status, timeout=timeout,
               message=f"message {message_id} {status} in db")
