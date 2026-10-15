import unittest

from tests.support import make_root
from ui.widgets import ConnectionBar, LogView, TableView


class LogViewTest(unittest.TestCase):
    def test_append_adds_timestamped_lines_and_keeps_text_intact(self):
        root = make_root(self)
        log = LogView(root)
        log.append("salut ă\nx")
        self.assertIn("salut ă\nx", log.contents())

    def test_old_lines_are_trimmed(self):
        root = make_root(self)
        log = LogView(root)
        LogView.MAX_LINES = 5
        self.addCleanup(setattr, LogView, "MAX_LINES", 2000)
        for index in range(20):
            log.append(f"linia {index}")
        self.assertNotIn("linia 0\n", log.contents())
        self.assertIn("linia 19", log.contents())


class TableViewTest(unittest.TestCase):
    def make(self):
        root = make_root(self)
        return TableView(root, [("A", 50), ("B", 50)])

    def test_upsert_puts_new_rows_first_and_updates_in_place(self):
        table = self.make()
        table.upsert("k1", ("1", "x"))
        table.upsert("k2", ("2", "y"))
        self.assertEqual([("2", "y"), ("1", "x")], table.rows())
        table.upsert("k1", ("1", "z"))
        self.assertEqual([("2", "y"), ("1", "z")], table.rows())

    def test_replace_keeps_selection_and_removes_missing_rows(self):
        table = self.make()
        table.replace([("a", ("1", "x")), ("b", ("2", "y"))])
        table.select("b")
        table.replace([("b", ("2", "yy")), ("c", ("3", "z"))])
        self.assertEqual("b", table.selected_key())
        self.assertEqual([("2", "yy"), ("3", "z")], table.rows())

    def test_numeric_looking_text_is_not_converted(self):
        table = self.make()
        table.upsert("k", ("007", "1e3"))
        self.assertEqual([("007", "1e3")], table.rows())

    def test_unicode_and_newlines_survive(self):
        table = self.make()
        table.upsert("k", ("ăîșțâ", "a\nb"))
        self.assertEqual([("ăîșțâ", "a\nb")], table.rows())

    def test_selected_key_is_none_without_selection(self):
        self.assertIsNone(self.make().selected_key())


class ConnectionBarTest(unittest.TestCase):
    def make(self):
        root = make_root(self)
        self.calls = []
        return ConnectionBar(root, "127.0.0.1", 50051, lambda: self.calls.append("connect"),
                             lambda: self.calls.append("disconnect"))

    def test_address_parses_host_and_port(self):
        bar = self.make()
        self.assertEqual(("127.0.0.1", 50051), bar.address())

    def test_invalid_address_raises(self):
        bar = self.make()
        for host, port in (("", "50051"), ("h", ""), ("h", "abc"), ("h", "0"), ("h", "70000"), ("h", "-5")):
            with self.subTest(host=host, port=port):
                bar.host.set(host)
                bar.port.set(port)
                with self.assertRaises(ValueError):
                    bar.address()

    def test_set_connected_toggles_buttons(self):
        bar = self.make()
        bar.set_connected(True)
        self.assertEqual("disabled", str(bar.connect_button.cget("state")))
        self.assertEqual("normal", str(bar.disconnect_button.cget("state")))
        bar.set_connected(False)
        self.assertEqual("normal", str(bar.connect_button.cget("state")))

    def test_buttons_invoke_callbacks(self):
        bar = self.make()
        bar.connect_button.invoke()
        bar.set_connected(True)
        bar.disconnect_button.invoke()
        self.assertEqual(["connect", "disconnect"], self.calls)


if __name__ == "__main__":
    unittest.main()
