import os
import signal
import socket
import subprocess
import sys
import threading
import time

CONSOLE_KIND = "console"
EXITED_KIND = "exited"


class PortInUseError(RuntimeError):
    def __init__(self, port):
        super().__init__(f"Portul {port} este ocupat. Opreste celalalt broker sau alege alt port.")
        self.port = port


def port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.5)
        return probe.connect_ex(("127.0.0.1", int(port))) == 0


def build_command(broker_project, options):
    return ["dotnet", "run", "--project", str(broker_project), "-c", "Release", "--", *options.arguments()]


def kill_tree(process):
    if process.poll() is not None:
        return
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True)
    else:
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)


def wait_until_ready(ping, alive, timeout=240, interval=0.5, clock=time.monotonic, sleep=time.sleep):
    deadline = clock() + timeout
    while clock() < deadline:
        if not alive():
            return False
        try:
            ping()
            return True
        except Exception:
            sleep(interval)
    return False


class BrokerProcess:
    def __init__(self, bus, broker_project, work_dir, popen=subprocess.Popen, killer=kill_tree,
                 port_check=port_in_use):
        self._bus = bus
        self._broker_project = broker_project
        self._work_dir = work_dir
        self._popen = popen
        self._killer = killer
        self._port_check = port_check
        self._process = None

    @property
    def running(self):
        return self._process is not None and self._process.poll() is None

    def start(self, options):
        if self.running:
            raise RuntimeError("brokerul ruleaza deja")
        if self._port_check(options.port):
            raise PortInUseError(options.port)
        extra = {} if sys.platform == "win32" else {"start_new_session": True}
        process = self._popen(
            build_command(self._broker_project, options),
            cwd=str(self._work_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            **extra,
        )
        self._process = process
        threading.Thread(target=self._pump, args=(process,), daemon=True).start()

    def stop(self):
        if self._process is not None:
            self._killer(self._process)

    def _pump(self, process):
        for line in process.stdout:
            self._bus.post(CONSOLE_KIND, line.rstrip())
        self._bus.post(EXITED_KIND, process.wait())
