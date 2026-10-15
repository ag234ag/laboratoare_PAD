import io
import os
import socket
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path

from core.broker_options import BrokerOptions
from core.broker_process import (BrokerProcess, PortInUseError, build_command, port_in_use,
                                 wait_until_ready)
from core.events import EventBus
from tests.support import unused_port

OPTIONS = BrokerOptions("50051", "persistent", "broker.db", "4")


class FakePopen:
    def __init__(self, command, finish=True, **kwargs):
        self.command = command
        self.kwargs = kwargs
        self.stdout = io.StringIO("linia 1\nlinia ă 2\n")
        self.pid = 4242
        self.finished = threading.Event()
        if finish:
            self.finished.set()

    def poll(self):
        return 0 if self.finished.is_set() else None

    def wait(self):
        self.finished.wait(5)
        return 0


def collect(bus, kinds, timeout=2):
    seen = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not all(kind in [k for k, _ in seen] for kind in kinds):
        seen.extend(bus.drain())
        time.sleep(0.01)
    return seen


class CommandTest(unittest.TestCase):
    def test_command_runs_the_broker_project_in_release(self):
        command = build_command(Path("C:/lab/broker"), OPTIONS)
        self.assertEqual(["dotnet", "run", "--project", str(Path("C:/lab/broker")), "-c", "Release", "--",
                          "--port", "50051", "--storage", "persistent", "--db", "broker.db", "--workers", "4"], command)


class PortTest(unittest.TestCase):
    def test_free_port_is_not_in_use(self):
        self.assertFalse(port_in_use(unused_port()))

    def test_listening_port_is_in_use(self):
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            server.listen()
            self.assertTrue(port_in_use(server.getsockname()[1]))


class BrokerProcessTest(unittest.TestCase):
    def make(self, finish=True, **overrides):
        self.bus = EventBus()
        self.launched = []

        def popen(command, **kwargs):
            process = FakePopen(command, finish=finish, **kwargs)
            self.launched.append(process)
            return process

        defaults = {"popen": popen, "killer": lambda process: self.killed.append(process), "port_check": lambda port: False}
        defaults.update(overrides)
        self.killed = []
        return BrokerProcess(self.bus, Path("C:/lab/broker"), Path("C:/lab"), **defaults)

    def test_start_streams_console_lines_then_reports_exit(self):
        process = self.make()
        process.start(OPTIONS)
        events = collect(self.bus, ["exited"])
        self.assertEqual([("console", "linia 1"), ("console", "linia ă 2"), ("exited", 0)], events)

    def test_start_uses_the_work_dir_and_utf8(self):
        process = self.make()
        process.start(OPTIONS)
        kwargs = self.launched[0].kwargs
        self.assertEqual(str(Path("C:/lab")), kwargs["cwd"])
        self.assertEqual("utf-8", kwargs["encoding"])
        self.assertEqual(subprocess.STDOUT, kwargs["stderr"])

    def test_second_start_while_running_is_refused(self):
        process = self.make(finish=False)
        process.start(OPTIONS)
        self.assertTrue(process.running)
        with self.assertRaises(RuntimeError):
            process.start(OPTIONS)
        self.assertEqual(1, len(self.launched))

    def test_port_in_use_blocks_the_start(self):
        process = self.make(port_check=lambda port: True)
        with self.assertRaises(PortInUseError):
            process.start(OPTIONS)
        self.assertEqual([], self.launched)

    def test_stop_calls_the_killer_with_the_running_process(self):
        process = self.make(finish=False)
        process.start(OPTIONS)
        process.stop()
        self.assertEqual(self.launched, self.killed)

    def test_stop_without_start_is_harmless(self):
        self.make().stop()


class WaitUntilReadyTest(unittest.TestCase):
    def test_returns_true_after_failed_pings(self):
        attempts = []

        def ping():
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("not yet")

        self.assertTrue(wait_until_ready(ping, lambda: True, timeout=10, sleep=lambda s: None))
        self.assertEqual(3, len(attempts))

    def test_returns_false_when_process_is_not_alive(self):
        self.assertFalse(wait_until_ready(lambda: None, lambda: False, timeout=10, sleep=lambda s: None))

    def test_returns_false_on_timeout(self):
        now = [0.0]

        def clock():
            return now[0]

        def sleep(seconds):
            now[0] += seconds

        def ping():
            raise ConnectionError()

        self.assertFalse(wait_until_ready(ping, lambda: True, timeout=2, interval=0.5, clock=clock, sleep=sleep))


class RealProcessTest(unittest.TestCase):
    def test_real_subprocess_output_is_streamed(self):
        bus = EventBus()

        def popen(command, **kwargs):
            environment = {**os.environ, "PYTHONIOENCODING": "utf-8"}
            return subprocess.Popen([sys.executable, "-c", "print('salut'); print('ă')"], env=environment, **kwargs)

        process = BrokerProcess(bus, Path("."), Path("."), popen=popen, port_check=lambda port: False)
        process.start(OPTIONS)
        events = collect(bus, ["exited"], timeout=10)
        self.assertEqual([("console", "salut"), ("console", "ă"), ("exited", 0)], events)


if __name__ == "__main__":
    unittest.main()
