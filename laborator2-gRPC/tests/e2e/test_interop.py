import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import oracle
from clients import wait_until
from expectations import expected_counts

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def subscriber_jar():
    subscriber_dir = ROOT / "subscriber"
    subprocess.run(["cmd", "/c", str(subscriber_dir / "mvnw.cmd"), "-q", "package", "-DskipTests"],
                   cwd=subscriber_dir, check=True)
    return subscriber_dir / "target" / "subscriber.jar"


class JavaSubscriber:
    def __init__(self, jar, port):
        self.process = subprocess.Popen(
            ["java", "-jar", str(jar), "127.0.0.1", str(port)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", bufsize=1,
        )
        self._lines = queue.Queue()
        self.output = ""
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self):
        for line in self.process.stdout:
            self._lines.put(line)

    def send(self, line):
        self.process.stdin.write(line + "\n")
        self.process.stdin.flush()

    def wait_for(self, text, timeout=30, count=1):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            while not self._lines.empty():
                self.output += self._lines.get()
            if self.output.count(text) >= count:
                return
            time.sleep(0.1)
        raise AssertionError(f"Java subscriber did not print {text!r} x{count}; output:\n{self.output}")

    def stop(self):
        if self.process.poll() is None:
            try:
                self.send("0")
                self.process.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                self.process.kill()


def run_publisher(port, script):
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    result = subprocess.run(
        [sys.executable, "-u", str(ROOT / "publisher" / "publisher.py"), "--port", str(port)],
        input=script, capture_output=True, text=True, encoding="utf-8", timeout=60, env=env,
    )
    return result.stdout


def test_python_publisher_and_java_subscriber_through_csharp_broker(persistent_broker, subscriber_jar):
    java = JavaSubscriber(subscriber_jar, persistent_broker.port)
    try:
        java.send("java-interop")
        java.send("1")
        java.send("interop")
        java.wait_for("[BROKER] Subscription confirmed: interop")

        output = run_publisher(persistent_broker.port, "1\ninterop\nSalut din Python\n0\n")
        assert "Subscribers found: 1" in output

        java.wait_for("Content    : Salut din Python")
        java.wait_for("[BROKER] ACK confirmed for:")

        diacritics = "Știri: școală, ăîâșț"
        output = run_publisher(persistent_broker.port, f"1\ninterop\n{diacritics}\n0\n")
        assert "Subscribers found: 1" in output
        java.wait_for(f"Content    : {diacritics}")

        java.send("5")
        java.wait_for("Auto-ACK is now OFF")
        output = run_publisher(persistent_broker.port, "1\ninterop\nFara ACK\n0\n")
        assert "Subscribers found: 1" in output

        def dead_message_ids():
            return [row[0] for row in oracle.messages(persistent_broker.db_path) if row[2] == "Fara ACK" and row[3] == "DEAD"]

        wait_until(lambda: len(dead_message_ids()) == 1, timeout=60, message="'Fara ACK' moved to DLQ")
        java.wait_for("Content    : Fara ACK", count=3)
        java.wait_for("[ACK disabled]", count=3)
    finally:
        java.stop()

    db_path = persistent_broker.db_path
    assert oracle.counts(db_path) == expected_counts(
        {"DELIVERED": 2, "DEAD": 1}, {"DELIVERED": 2, "DEAD": 1}, dlq_delivery=1)
    assert oracle.conservation_violations(db_path) == []
