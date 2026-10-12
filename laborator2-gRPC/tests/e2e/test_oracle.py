import sqlite3
from contextlib import closing

import pytest

import oracle

SCHEMA = """
CREATE TABLE Messages
(
    MessageId TEXT PRIMARY KEY,
    Topic TEXT NOT NULL,
    Content TEXT NOT NULL,
    Status TEXT NOT NULL DEFAULT 'PENDING',
    CreatedAt TEXT NOT NULL
);
CREATE TABLE Subscriptions
(
    SubscriberId TEXT NOT NULL,
    Topic TEXT NOT NULL,
    CreatedAt TEXT NOT NULL,
    PRIMARY KEY (SubscriberId, Topic)
);
CREATE TABLE Deliveries
(
    MessageId TEXT NOT NULL,
    SubscriberId TEXT NOT NULL,
    Status TEXT NOT NULL DEFAULT 'PENDING',
    RetryCount INTEGER NOT NULL DEFAULT 0,
    LastAttemptAt TEXT,
    LastError TEXT,
    PRIMARY KEY (MessageId, SubscriberId)
);
CREATE TABLE DeadLetters
(
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    MessageId TEXT,
    SubscriberId TEXT,
    Topic TEXT,
    RawPayload TEXT,
    Reason TEXT NOT NULL,
    RetryCount INTEGER NOT NULL DEFAULT 0,
    CreatedAt TEXT NOT NULL
);
"""


def build_db(tmp_path, messages=(), subscriptions=(), deliveries=(), dead_letters=()):
    path = tmp_path / "synthetic.db"
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(SCHEMA)
        connection.executemany("INSERT INTO Messages VALUES (?, ?, 'c', ?, 't')", messages)
        connection.executemany("INSERT INTO Subscriptions VALUES (?, ?, 't')", subscriptions)
        connection.executemany("INSERT INTO Deliveries (MessageId, SubscriberId, Status) VALUES (?, ?, ?)", deliveries)
        connection.executemany(
            "INSERT INTO DeadLetters (MessageId, SubscriberId, Reason, CreatedAt) VALUES (?, ?, 'r', 't')",
            dead_letters)
        connection.commit()
    return path


def test_consistent_db_with_unsubscribed_pending_delivery_has_no_violations(tmp_path):
    db = build_db(
        tmp_path,
        messages=[("m1", "a", "DELIVERED"), ("m2", "a", "PENDING"), ("m3", "a", "DEAD")],
        subscriptions=[("s1", "a")],
        deliveries=[
            ("m1", "s1", "DELIVERED"),
            ("m2", "s1", "PENDING"),
            ("m2", "gone", "PENDING"),
            ("m3", "s1", "DEAD"),
        ],
        dead_letters=[("m3", "s1")],
    )
    assert oracle.conservation_violations(db) == []


@pytest.mark.parametrize(
    "case, build, expected_fragment",
    [
        (
            "message status inconsistent with deliveries",
            dict(messages=[("m1", "a", "PENDING")], subscriptions=[("s1", "a")],
                 deliveries=[("m1", "s1", "DELIVERED")]),
            "deliveries imply DELIVERED",
        ),
        (
            "DEAD delivery without DLQ row",
            dict(messages=[("m1", "a", "DEAD")], subscriptions=[("s1", "a")],
                 deliveries=[("m1", "s1", "DEAD")]),
            "do not match DLQ rows",
        ),
        (
            "DLQ row whose delivery is not DEAD",
            dict(messages=[("m1", "a", "DELIVERED")], subscriptions=[("s1", "a")],
                 deliveries=[("m1", "s1", "DELIVERED")], dead_letters=[("m1", "s1")]),
            "do not match DLQ rows",
        ),
        (
            "subscription without delivery for existing message",
            dict(messages=[("m1", "a", "PENDING")], subscriptions=[("s1", "a")]),
            "has no delivery for message m1",
        ),
        (
            "delivery referencing missing message",
            dict(deliveries=[("ghost", "s1", "PENDING")]),
            "references a missing message",
        ),
    ],
)
def test_oracle_reports_violation(tmp_path, case, build, expected_fragment):
    problems = oracle.conservation_violations(build_db(tmp_path, **build))
    assert len(problems) == 1, (case, problems)
    assert any(expected_fragment in problem for problem in problems), problems
