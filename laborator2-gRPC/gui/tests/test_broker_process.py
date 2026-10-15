import io
import os
import socket
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from core import broker_process

from core.broker_options import BrokerOptions
from core.broker_process import (BrokerProcess, PortInUseError, build_command, kill_tree, port_in_use,
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


class BlockingStream:
    def __init__(self):
        self.release = threading.Event()
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        self.release.wait(30)
        raise StopIteration

    def close(self):
        self.release.wait(30)
        self.closed = True


class GatedBus(EventBus):
    def __init__(self):
        super().__init__()
        self.gate = threading.Event()
        self.reached = threading.Event()

    def post(self, kind, payload):
        if kind == "exited":
            self.reached.set()
            self.gate.wait(5)
        super().post(kind, payload)


def wait_for(condition, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


def pid_alive(pid):
    if sys.platform == "win32":
        output = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True,
                                errors="replace").stdout
        return str(pid) in output.split()
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


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

    def test_exited_is_posted_even_when_the_pipe_never_closes(self):
        stream = BlockingStream()
        self.addCleanup(stream.release.set)

        def popen(command, **kwargs):
            process = FakePopen(command, **kwargs)
            process.stdout = stream
            return process

        process = self.make(popen=popen)
        process.start(OPTIONS)
        self.assertEqual([("exited", 0)], collect(self.bus, ["exited"], timeout=6))

    def test_stdout_is_closed_when_exited_is_observed(self):
        process = self.make()
        process.start(OPTIONS)
        self.assertIn(("exited", 0), collect(self.bus, ["exited"]))
        self.assertTrue(self.launched[0].stdout.closed)

    def test_blocked_reader_closes_its_pipe_quietly_once_the_pipe_ends(self):
        stream = BlockingStream()
        self.addCleanup(stream.release.set)
        errors = []

        def popen(command, **kwargs):
            process = FakePopen(command, **kwargs)
            process.stdout = stream
            return process

        with mock.patch.object(threading, "excepthook", lambda args: errors.append(args.exc_value)):
            process = self.make(popen=popen)
            process.start(OPTIONS)
            self.assertEqual([("exited", 0)], collect(self.bus, ["exited"], timeout=6))
            self.assertTrue(wait_for(lambda: not process.running))
            self.assertFalse(stream.closed)
            stream.release.set()
            self.assertTrue(wait_for(lambda: stream.closed))
        self.assertEqual([], errors)

    def test_running_stays_true_until_exited_is_posted(self):
        bus = GatedBus()
        self.addCleanup(bus.gate.set)
        process = BrokerProcess(bus, Path("C:/lab/broker"), Path("C:/lab"),
                                popen=lambda command, **kwargs: FakePopen(command, **kwargs),
                                port_check=lambda port: False)
        process.start(OPTIONS)
        self.assertTrue(bus.reached.wait(5))
        self.assertTrue(process.running)
        with self.assertRaises(RuntimeError):
            process.start(OPTIONS)
        bus.gate.set()
        self.assertTrue(wait_for(lambda: not process.running))
        self.assertIn(("exited", 0), bus.drain())

    def test_console_lines_keep_leading_and_trailing_spaces(self):
        def popen(command, **kwargs):
            process = FakePopen(command, **kwargs)
            process.stdout = io.StringIO("  indentat  \r\n")
            return process

        process = self.make(popen=popen)
        process.start(OPTIONS)
        self.assertEqual([("console", "  indentat  "), ("exited", 0)], collect(self.bus, ["exited"]))


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


class KillTreeTest(unittest.TestCase):
    def kill_with_taskkill_result(self, returncode, stdout=b"", stderr=b""):
        process = mock.Mock()
        process.poll.return_value = None
        process.pid = 77
        result = subprocess.CompletedProcess([], returncode, stdout, stderr)
        with mock.patch.object(broker_process.sys, "platform", "win32"),                 mock.patch.object(broker_process.subprocess, "run", return_value=result) as run:
            kill_tree(process)
        return run

    def test_taskkill_is_called_for_the_whole_tree(self):
        run = self.kill_with_taskkill_result(0)
        self.assertEqual(["taskkill", "/PID", "77", "/T", "/F"], run.call_args.args[0])

    def test_process_not_found_is_accepted(self):
        self.kill_with_taskkill_result(128)

    def test_other_taskkill_failures_raise_with_code_and_output(self):
        with self.assertRaises(RuntimeError) as context:
            self.kill_with_taskkill_result(1, stderr="Acces refuzat".encode())
        self.assertIn("1", str(context.exception))
        self.assertIn("Acces refuzat", str(context.exception))

    def test_already_finished_process_is_left_alone(self):
        process = mock.Mock()
        process.poll.return_value = 0
        with mock.patch.object(broker_process.subprocess, "run") as run:
            kill_tree(process)
        run.assert_not_called()

    def test_real_process_tree_is_killed(self):
        script = ("import subprocess, sys, time\n"
                  "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
                  "print(child.pid, flush=True)\n"
                  "time.sleep(60)\n")
        extra = {} if sys.platform == "win32" else {"start_new_session": True}
        parent = subprocess.Popen([sys.executable, "-c", script], stdout=subprocess.PIPE, text=True, **extra)
        child_pid = None

        def cleanup():
            for pid in (parent.pid, child_pid):
                if pid is not None and pid_alive(pid):
                    if sys.platform == "win32":
                        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True)
                    else:
                        os.kill(pid, 9)
            parent.wait(10)
            parent.stdout.close()

        self.addCleanup(cleanup)
        child_pid = int(parent.stdout.readline())
        self.assertTrue(pid_alive(child_pid))
        kill_tree(parent)
        parent.wait(5)
        self.assertTrue(wait_for(lambda: not pid_alive(parent.pid) and not pid_alive(child_pid)))


class RealProcessTest(unittest.TestCase):
    def test_real_subprocess_output_is_streamed(self):
        bus = EventBus()
        launched = []

        def popen(command, **kwargs):
            environment = {**os.environ, "PYTHONIOENCODING": "utf-8"}
            launched.append(subprocess.Popen([sys.executable, "-c", "print('salut'); print('ă')"], env=environment,
                                             **kwargs))
            return launched[0]

        process = BrokerProcess(bus, Path("."), Path("."), popen=popen, port_check=lambda port: False)
        process.start(OPTIONS)
        events = collect(bus, ["exited"], timeout=10)
        self.assertEqual([("console", "salut"), ("console", "ă"), ("exited", 0)], events)
        self.assertTrue(launched[0].stdout.closed)


if __name__ == "__main__":
    unittest.main()
