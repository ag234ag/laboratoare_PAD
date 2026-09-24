import sqlite3

from panels import StatePanel
from support import TkTestCase

STATE = {
    "Messages": [("aaaaaaaa", "sport", "goal", "PENDING")],
    "Deliveries": [("aaaaaaaa", "S1", "PENDING", 1, ""), ("aaaaaaaa", "S2", "DEAD", 3, "x")],
    "DeadLetters": [],
}


class StatePanelTest(TkTestCase):
    def counts(self, panel):
        return {name: len(table.get_children()) for name, table in panel.tables.items()}

    def test_shows_rows_from_reader(self):
        panel = StatePanel(self.root, lambda path: STATE, "x.db")
        self.assertEqual(self.counts(panel), {"Messages": 1, "Deliveries": 2, "DeadLetters": 0})
        self.assertEqual(panel.status.get(), "ok")

    def test_refresh_replaces_rows_instead_of_appending(self):
        panel = StatePanel(self.root, lambda path: STATE, "x.db")
        panel.refresh()
        panel.refresh()
        self.assertEqual(self.counts(panel)["Messages"], 1)

    def test_missing_database_shows_status_and_keeps_previous_rows(self):
        answers = [STATE, FileNotFoundError("x.db")]

        def reader(path):
            answer = answers.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return answer

        panel = StatePanel(self.root, reader, "x.db")
        panel.refresh()
        self.assertEqual(panel.status.get(), "broker.db negasit")
        self.assertEqual(self.counts(panel)["Messages"], 1)

    def test_sqlite_error_shows_status_without_crashing(self):
        def reader(path):
            raise sqlite3.OperationalError("database is locked")

        panel = StatePanel(self.root, reader, "x.db")
        self.assertIn("database is locked", panel.status.get())

    def test_path_entry_change_is_used_on_next_refresh(self):
        seen = []

        def reader(path):
            seen.append(path)
            return STATE

        panel = StatePanel(self.root, reader, "first.db")
        panel.db_path.set("second.db")
        panel.refresh()
        self.assertEqual(seen[-1], "second.db")
