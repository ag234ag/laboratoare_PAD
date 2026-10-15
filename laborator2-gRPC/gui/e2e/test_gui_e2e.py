import gc
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
import tkinter as tk
import unittest
from contextlib import closing
from pathlib import Path

from broker_app import BrokerApp
from core.broker_process import port_in_use
from publisher_app import PublisherApp
from receiver_app import ReceiverApp
from tests.support import unused_port

GUI_DIR = Path(__file__).resolve().parents[1]
LAB_DIR = GUI_DIR.parent
sys.path.insert(0, str(LAB_DIR / "tests" / "e2e"))

import oracle  # noqa: E402

STOPPED = re.compile(r"Oprit \(cod -?\d+\)")
PROCESS_QUERY = ("Get-CimInstance Win32_Process | ForEach-Object "
                 "{ '{0},{1},{2},{3}' -f $_.ProcessId, $_.ParentProcessId, $_.CreationDate.Ticks, $_.Name }")


def pump(root, condition, timeout, message):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        root.update()
        if condition():
            return
        time.sleep(0.05)
    raise AssertionError(f"{message} nu s-a indeplinit in {timeout}s")


def process_table():
    result = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", PROCESS_QUERY],
                            capture_output=True, text=True, check=True)
    table = {}
    for line in result.stdout.splitlines():
        parts = line.strip().split(",", 3)
        if len(parts) == 4 and parts[0].isdigit() and parts[1].isdigit():
            table[int(parts[0])] = (int(parts[1]), int(parts[2]) if parts[2].isdigit() else 0, parts[3])
    return table


def process_tree(root_pid):
    table = process_table()
    tree = {}
    frontier = [root_pid]
    while frontier:
        pid = frontier.pop()
        if pid not in table or pid in tree:
            continue
        tree[pid] = table[pid]
        frontier.extend(child for child, (parent, created, _) in table.items()
                        if parent == pid and created >= table[pid][1])
    return tree


def survivors(tree):
    table = process_table()
    return sorted((pid, name) for pid, (_, created, name) in tree.items()
                  if pid in table and table[pid][1] == created)


def subscriptions(db):
    uri = f"{Path(db).resolve().as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=5)) as connection:
        return connection.execute("SELECT SubscriberId, Topic FROM Subscriptions").fetchall()


def broker_pid(broker):
    return broker._process._process.pid


@unittest.skipUnless(sys.platform == "win32", "arborele de procese se verifica prin Win32_Process")
class GuiEndToEnd(unittest.TestCase):
    def test_demo_scenario_matches_the_database(self):
        try:
            root = tk.Tk()
        except tk.TclError as error:
            self.skipTest(f"fara display: {error}")
        root.withdraw()
        self.clients = []
        self.timings = {}
        broker = None
        try:
            broker = BrokerApp(root, LAB_DIR / "broker", LAB_DIR)
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as folder:
                port = unused_port()
                try:
                    self.run_lifecycle(root, broker, Path(folder) / "broker.db", port)
                finally:
                    self.shutdown(root, broker, port)
            print(f"\ndurate (s): {self.timings}", file=sys.stderr)
        finally:
            broker = None
            self.clients = None
            gc.collect()
            root.destroy()
            root = None
            gc.collect()

    def run_lifecycle(self, root, broker, db, port):
        listening = f"Listening on 0.0.0.0:{port}"
        broker.port.set(str(port))
        broker.storage.set("persistent")
        broker.db.set(str(db))
        broker.workers.set("4")
        tree = self.start_broker(root, broker, "start")
        self.assertEqual(1, broker.console.contents().count(listening))
        self.assertTrue(any(name.lower() == "broker.exe" for _, _, name in tree.values()), tree)

        began = time.monotonic()
        self.run_scenario(root, broker, db, port)
        self.timings["scenariu"] = round(time.monotonic() - began, 1)

        began = time.monotonic()
        broker.stop()
        self.assert_stopped(root, broker, port, tree, 20)
        self.timings["stop"] = round(time.monotonic() - began, 1)
        for receiver in self.clients[:2]:
            pump(root, lambda r=receiver: r.bar.status.get() == "Deconectat", 20, "receiver deconectat la oprire")

        tree = self.start_broker(root, broker, "restart")
        self.assertEqual(2, broker.console.contents().count(listening))

        began = time.monotonic()
        killed = subprocess.run(["taskkill", "/PID", str(broker_pid(broker)), "/T", "/F"], capture_output=True)
        self.assertEqual(0, killed.returncode, killed.stdout + killed.stderr)
        self.assert_stopped(root, broker, port, tree, 10)
        self.timings["moarte externa"] = round(time.monotonic() - began, 1)

    def start_broker(self, root, broker, phase):
        began = time.monotonic()
        broker.start()
        pump(root, lambda: broker.status.get() == "Running", 240, f"brokerul Running ({phase})")
        self.timings[phase] = round(time.monotonic() - began, 1)
        self.assertTrue(port_in_use(broker.port.get()))
        tree = process_tree(broker_pid(broker))
        self.assertEqual(sorted((pid, name) for pid, (_, _, name) in tree.items()), survivors(tree))
        print(f"\narbore {phase}: {survivors(tree)}", file=sys.stderr)
        return tree

    def assert_stopped(self, root, broker, port, tree, timeout):
        pump(root, lambda: STOPPED.fullmatch(broker.status.get()) is not None, timeout,
             f"status 'Oprit (cod N)', acum '{broker.status.get()}'")
        pump(root, lambda: not port_in_use(port), 20, "portul brokerului eliberat")
        pump(root, lambda: not survivors(tree), 10, f"arborele {sorted(tree)} oprit")

    def shutdown(self, root, broker, port):
        try:
            broker.stop()
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and port_in_use(port):
                root.update()
                time.sleep(0.1)
            self.assertFalse(port_in_use(port),
                             f"portul {port} ramane ocupat dupa oprire; consola brokerului: {broker.console.contents()[-500:]}")
        finally:
            for client in self.clients:
                client.close()

    def run_scenario(self, root, broker, db, port):
        receiver_a = ReceiverApp(tk.Toplevel(root), "127.0.0.1", port)
        receiver_b = ReceiverApp(tk.Toplevel(root), "127.0.0.1", port)
        publisher = PublisherApp(tk.Toplevel(root), "127.0.0.1", port)
        self.clients.extend((receiver_a, receiver_b, publisher))
        receiver_a.subscriber_id.set("gui-a")
        receiver_b.subscriber_id.set("gui-b")
        receiver_b.auto_ack.set(False)
        for receiver in (receiver_a, receiver_b):
            receiver.connect()
            pump(root, lambda r=receiver: r.bar.status.get().startswith("Listen"), 20, "receiver conectat")
            receiver.topic.set("sport")
            receiver.subscribe()
            pump(root, lambda r=receiver: "sport" in r.topics_text.get(), 20, "abonare")
        publisher.connect()
        pump(root, lambda: publisher.bar.status.get().startswith("Conectat"), 20, "publisher conectat")

        for text in ("meci unu", "meci doi", "meci trei"):
            publisher.topic.set("sport")
            publisher.content.set(text)
            publisher.publish()
        publisher.topic.set("nimeni")
        publisher.content.set("fara abonat")
        publisher.publish()
        publisher.publish_without_topic()
        publisher.publish_without_content()
        pump(root, lambda: len(publisher.history.rows()) == 6, 20, "6 raspunsuri publisher")
        publisher.resend_last()
        pump(root, lambda: len(publisher.history.rows()) == 7, 20, "al 7-lea raspuns")

        pump(root, lambda: len(receiver_a.table.rows()) == 3
             and all(row[4] == "ACK trimis" for row in receiver_a.table.rows()), 30, "receiver A a confirmat 3")
        pump(root, lambda: len(receiver_b.table.rows()) == 3
             and all(row[3] == "3/3" for row in receiver_b.table.rows()), 60, "receiver B a primit 3 incercari")
        pump(root, lambda: len(broker.dead_letters_table.rows()) == 5, 60, "5 randuri in DLQ")
        pump(root, lambda: len(broker.deliveries_table.rows()) == 6
             and sorted(row[2] for row in broker.deliveries_table.rows()) == ["DEAD"] * 3 + ["DELIVERED"] * 3,
             30, "livrari in tabel")

        history = publisher.history.rows()
        labels = [row[3] for row in history]
        buckets = {
            "ok": sum(1 for label in labels if label.startswith("OK")),
            "invalid": labels.count("INVALID -> DLQ"),
            "duplicate": labels.count("DUPLICATE"),
        }
        self.assertEqual({"ok": 4, "invalid": 2, "duplicate": 1}, buckets)
        self.assertEqual(len(labels), sum(buckets.values()), "raspuns publisher fara galeata")
        self.assertEqual(["OK, 0 abonati"] + ["OK, 2 abonati"] * 3, sorted(label for label in labels if label.startswith("OK")))

        counts = oracle.counts(db)
        self.assertEqual([], oracle.conservation_violations(db))
        self.assertEqual(buckets["ok"], sum(counts["messages"].values()))
        self.assertEqual({"DELIVERED": 3, "PENDING": 1}, counts["messages"])
        self.assertEqual({"DELIVERED": 3, "DEAD": 3}, counts["deliveries"])
        self.assertEqual(buckets["invalid"], counts["dead_letters_invalid"])
        self.assertEqual(counts["deliveries"]["DEAD"], counts["dead_letters_delivery"])

        db_messages = {row[0]: tuple(row[1:4]) for row in oracle.messages(db)}
        gui_messages = {row[0]: tuple(row[1:4]) for row in broker.messages_table.rows()}
        self.assertEqual(db_messages, gui_messages)

        accepted = {row[1] for row in history if row[3].startswith("OK")}
        duplicates = [row[1] for row in history if row[3] == "DUPLICATE"]
        self.assertEqual(set(db_messages), accepted)
        self.assertEqual(1, len(duplicates))
        self.assertEqual(("nimeni", "fara abonat", "PENDING"), db_messages[duplicates[0]])

        gui_deliveries = {(row[0], row[1]): (row[2], row[3]) for row in broker.deliveries_table.rows()}
        for (message_id, subscriber_id), shown in gui_deliveries.items():
            status, retries = oracle.delivery(db, message_id, subscriber_id)
            self.assertEqual((status, str(retries)), shown)
        self.assertEqual(6, len(gui_deliveries))

        for receiver, expected in ((receiver_a, "DELIVERED"), (receiver_b, "DEAD")):
            subscriber_id = receiver.subscriber_id.get()
            for message_id, topic, content, _, _ in receiver.table.rows():
                self.assertEqual((topic, content), db_messages[message_id][:2])
                self.assertEqual(expected, oracle.delivery(db, message_id, subscriber_id)[0])
            received = {row[0] for row in receiver.table.rows()}
            self.assertEqual({m for m, s in gui_deliveries if s == subscriber_id}, received)

        db_dlq = sorted((row[0] or "", row[1] or "", row[2] or "", row[3] or "", row[4], str(row[5]))
                        for row in oracle.dead_letters(db))
        gui_dlq = sorted((row[1], row[2], row[3], row[7], row[4], row[5]) for row in broker.dead_letters_table.rows())
        self.assertEqual(db_dlq, gui_dlq)
        self.assertTrue(all(row[3] for row in db_dlq if not row[0]), db_dlq)

        db_subscriptions = subscriptions(db)
        self.assertEqual({("gui-a", "sport"), ("gui-b", "sport")}, set(db_subscriptions))
        self.assertEqual(sorted(db_subscriptions), sorted(broker.subscriptions_table.rows()))
        online = broker.online.get().split(": ", 1)[1].split(", ")
        self.assertEqual({"gui-a", "gui-b"}, set(online))


if __name__ == "__main__":
    unittest.main()
