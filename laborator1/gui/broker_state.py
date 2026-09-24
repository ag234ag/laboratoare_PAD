import sqlite3
from pathlib import Path

TABLE_COLUMNS = {
    "Messages": ("MessageId", "Topic", "Content", "Status"),
    "Deliveries": ("MessageId", "SubscriberId", "Status", "RetryCount", "LastError"),
    "DeadLetters": ("MessageId", "SubscriberId", "Reason", "RetryCount"),
}

_QUERIES = {
    "Messages": (
        "SELECT substr(MessageId, 1, 8), Topic, Content, Status "
        "FROM Messages ORDER BY CreatedAt"
    ),
    "Deliveries": (
        "SELECT substr(MessageId, 1, 8), SubscriberId, Status, RetryCount, COALESCE(LastError, '') "
        "FROM Deliveries ORDER BY rowid"
    ),
    "DeadLetters": (
        "SELECT COALESCE(substr(MessageId, 1, 8), ''), COALESCE(SubscriberId, ''), Reason, RetryCount "
        "FROM DeadLetters ORDER BY Id"
    ),
}


def read_state(db_path):
    path = Path(db_path)
    if not path.is_file():
        raise FileNotFoundError(db_path)
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    try:
        return {name: connection.execute(sql).fetchall() for name, sql in _QUERIES.items()}
    finally:
        connection.close()
