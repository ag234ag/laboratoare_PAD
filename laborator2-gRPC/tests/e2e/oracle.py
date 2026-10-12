import sqlite3
from contextlib import closing
from pathlib import Path

STATUSES = ("PENDING", "DELIVERED", "DEAD")


def _snapshot(db_path, queries):
    uri = f"{Path(db_path).resolve().as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=5, isolation_level=None)) as connection:
        connection.execute("BEGIN")
        try:
            return [connection.execute(sql, params).fetchall() for sql, params in queries]
        finally:
            connection.execute("ROLLBACK")


def _rows(db_path, sql, params=()):
    return _snapshot(db_path, [(sql, params)])[0]


def message_status(db_path, message_id):
    rows = _rows(db_path, "SELECT Status FROM Messages WHERE MessageId = ?", (message_id,))
    return rows[0][0] if rows else None


def messages(db_path):
    return _rows(db_path, "SELECT MessageId, Topic, Content, Status FROM Messages")


def delivery(db_path, message_id, subscriber_id):
    rows = _rows(
        db_path,
        "SELECT Status, RetryCount FROM Deliveries WHERE MessageId = ? AND SubscriberId = ?",
        (message_id, subscriber_id),
    )
    return rows[0] if rows else None


def dead_letters(db_path):
    return _rows(db_path, "SELECT MessageId, SubscriberId, Topic, RawPayload, Reason, RetryCount FROM DeadLetters ORDER BY Id")


def counts(db_path):
    by_message, by_delivery, invalid, from_delivery = _snapshot(db_path, [
        ("SELECT Status, COUNT(*) FROM Messages GROUP BY Status", ()),
        ("SELECT Status, COUNT(*) FROM Deliveries GROUP BY Status", ()),
        ("SELECT COUNT(*) FROM DeadLetters WHERE MessageId IS NULL", ()),
        ("SELECT COUNT(*) FROM DeadLetters WHERE MessageId IS NOT NULL", ()),
    ])
    return {
        "messages": dict(by_message),
        "deliveries": dict(by_delivery),
        "dead_letters_invalid": invalid[0][0],
        "dead_letters_delivery": from_delivery[0][0],
    }


def _expected_message_status(delivery_statuses):
    if not delivery_statuses or "PENDING" in delivery_statuses:
        return "PENDING"
    if "DELIVERED" in delivery_statuses:
        return "DELIVERED"
    return "DEAD"


def conservation_violations(db_path):
    problems = []
    message_rows, delivery_rows, dlq_rows, uncovered_rows = _snapshot(db_path, [
        ("SELECT MessageId, Status FROM Messages", ()),
        ("SELECT MessageId, SubscriberId, Status FROM Deliveries", ()),
        ("SELECT MessageId, SubscriberId FROM DeadLetters WHERE MessageId IS NOT NULL", ()),
        (
            "SELECT m.MessageId, s.SubscriberId FROM Messages m "
            "JOIN Subscriptions s ON s.Topic = m.Topic "
            "LEFT JOIN Deliveries d ON d.MessageId = m.MessageId AND d.SubscriberId = s.SubscriberId "
            "WHERE d.MessageId IS NULL",
            (),
        ),
    ])
    dlq_pairs = sorted(dlq_rows)
    for message_id, subscriber_id in uncovered_rows:
        problems.append(f"subscription {subscriber_id} has no delivery for message {message_id}")

    statuses_by_message = {}
    for message_id, _, status in delivery_rows:
        statuses_by_message.setdefault(message_id, []).append(status)

    message_buckets = {status: 0 for status in STATUSES}
    for message_id, status in message_rows:
        if status not in STATUSES:
            problems.append(f"message {message_id} has unknown status {status}")
            continue
        message_buckets[status] += 1
        expected = _expected_message_status(statuses_by_message.get(message_id, []))
        if status != expected:
            problems.append(f"message {message_id} is {status} but its deliveries imply {expected}")
    if sum(message_buckets.values()) != len(message_rows):
        problems.append(f"messages: {len(message_rows)} rows but buckets {message_buckets}")

    known_messages = {message_id for message_id, _ in message_rows}
    delivery_buckets = {status: 0 for status in STATUSES}
    for message_id, subscriber_id, status in delivery_rows:
        if message_id not in known_messages:
            problems.append(f"delivery {message_id}/{subscriber_id} references a missing message")
        if status not in STATUSES:
            problems.append(f"delivery {message_id}/{subscriber_id} has unknown status {status}")
        else:
            delivery_buckets[status] += 1
    if sum(delivery_buckets.values()) != len(delivery_rows):
        problems.append(f"deliveries: {len(delivery_rows)} rows but buckets {delivery_buckets}")

    dead_pairs = sorted((m, s) for m, s, status in delivery_rows if status == "DEAD")
    if dead_pairs != dlq_pairs:
        problems.append(f"DEAD deliveries {dead_pairs} do not match DLQ rows {dlq_pairs}")
    return problems
