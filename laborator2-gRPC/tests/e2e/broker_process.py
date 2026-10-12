import subprocess
import time

import grpc

from clients import broker_pb2, broker_pb2_grpc


class BrokerProcess:
    def __init__(self, dll, storage, db_path, port, work_dir):
        self.dll = dll
        self.storage = storage
        self.db_path = db_path
        self.port = port
        self.work_dir = work_dir
        self.log_path = work_dir / f"broker-{port}.log"
        self._process = None
        self._log = None

    @property
    def address(self):
        return f"127.0.0.1:{self.port}"

    def start(self, timeout=30):
        self._log = open(self.log_path, "a", encoding="utf-8")
        self._process = subprocess.Popen(
            ["dotnet", str(self.dll), "--port", str(self.port), "--storage", self.storage, "--db", str(self.db_path)],
            cwd=self.work_dir,
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                self._close_log()
                raise RuntimeError(f"broker exited early, see {self.log_path}")
            try:
                with grpc.insecure_channel(self.address) as channel:
                    broker_pb2_grpc.MessageBrokerStub(channel).Ping(broker_pb2.PingRequest(), timeout=1)
                return
            except grpc.RpcError:
                time.sleep(0.2)
        self.stop()
        raise TimeoutError(f"broker did not answer Ping within {timeout}s, see {self.log_path}")

    def stop(self):
        if self._process is not None and self._process.poll() is None:
            self._process.kill()
            self._process.wait(timeout=10)
        self._close_log()

    def _close_log(self):
        if self._log is not None:
            self._log.close()
            self._log = None

    def restart(self):
        self.stop()
        self.start()

    def log_text(self):
        return self.log_path.read_text(encoding="utf-8", errors="replace")
