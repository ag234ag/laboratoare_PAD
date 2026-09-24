import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from broker_state import TABLE_COLUMNS, read_state

SCHEMA = """
CREATE TABLE Messages (MessageId TEXT PRIMARY KEY, Topic TEXT NOT NULL, Content TEXT NOT NULL,
    Status TEXT NOT NULL DEFAULT 'PENDING', CreatedAt TEXT NOT NULL);
CREATE TABLE Deliveries (MessageId TEXT NOT NULL, SubscriberId TEXT NOT NULL,
    Status TEXT NOT NULL DEFAULT 'PENDING', RetryCount INTEGER NOT NULL DEFAULT 0,
    LastAttemptAt TEXT, LastError TEXT, PRIMARY KEY (MessageId, SubscriberId));
CREATE TABLE DeadLetters (Id INTEGER PRIMARY KEY AUTOINCREMENT, MessageId TEXT, SubscriberId TEXT,
    Topic TEXT, RawPayload TEXT, Reason TEXT NOT NULL, RetryCount INTEGER NOT NULL DEFAULT 0,
    CreatedAt TEXT NOT NULL);
"""


class BrokerStateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "my dir" / "broker.db"
        self.path.parent.mkdir()

    def create_db(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.executescript(SCHEMA)
            db.execute("INSERT INTO Messages VALUES ('aaaaaaaa-1111', 'sport', 'salut ăț', 'DELIVERED', '2026-01-01')")
            db.execute("INSERT INTO Messages VALUES ('bbbbbbbb-2222', 'news', 'x', 'PENDING', '2026-01-02')")
            db.execute("INSERT INTO Deliveries VALUES ('aaaaaaaa-1111', 'S1', 'DELIVERED', 1, NULL, NULL)")
            db.execute("INSERT INTO Deliveries VALUES ('bbbbbbbb-2222', 'S2', 'PENDING', 2, '2026-01-02', 'boom')")
            db.execute("INSERT INTO DeadLetters (MessageId, SubscriberId, Topic, RawPayload, Reason, RetryCount, CreatedAt) "
                       "VALUES (NULL, NULL, NULL, '{bad', 'INVALID_JSON', 0, '2026-01-03')")
            db.commit()

    def test_reads_all_three_tables_with_declared_column_count(self):
        self.create_db()
        state = read_state(str(self.path))
        self.assertEqual(list(state), ["Messages", "Deliveries", "DeadLetters"])
        for name, rows in state.items():
            for row in rows:
                self.assertEqual(len(row), len(TABLE_COLUMNS[name]))

    def test_rows_are_truncated_ids_and_never_none(self):
        self.create_db()
        state = read_state(str(self.path))
        self.assertEqual(state["Messages"][1], ("aaaaaaaa", "sport", "salut ăț", "DELIVERED"))
        self.assertEqual(state["Deliveries"][0], ("bbbbbbbb", "S2", "PENDING", 2, "boom"))
        self.assertEqual(state["Deliveries"][1][4], "")
        self.assertEqual(state["DeadLetters"][0], ("", "", "INVALID_JSON", 0))

    def test_newest_rows_come_first(self):
        self.create_db()
        state = read_state(str(self.path))
        self.assertEqual(state["Messages"][0][0], "bbbbbbbb")
        self.assertEqual(state["Deliveries"][0][0], "bbbbbbbb")

    def test_missing_file_raises_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            read_state(str(self.path))

    def test_garbage_file_raises_sqlite_error(self):
        self.path.write_bytes(b"this is not a database" * 100)
        with self.assertRaises(sqlite3.Error):
            read_state(str(self.path))

    def test_database_without_tables_raises_sqlite_error(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("CREATE TABLE other (x INTEGER)")
            db.commit()
        with self.assertRaises(sqlite3.Error):
            read_state(str(self.path))

    def test_connection_is_read_only(self):
        self.create_db()
        read_state(str(self.path))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM Messages").fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()
