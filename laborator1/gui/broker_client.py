import json
import socket
import threading


class BrokerClient:
    CONNECT_TIMEOUT = 3

    def __init__(self, events):
        self.events = events
        self._sock = None
        self._send_lock = threading.Lock()

    @property
    def connected(self):
        return self._sock is not None

    def connect(self, host, port):
        self.disconnect()
        sock = socket.create_connection((host, port), timeout=self.CONNECT_TIMEOUT)
        sock.settimeout(None)
        self._sock = sock
        threading.Thread(target=self._read_loop, args=(sock,), daemon=True).start()

    def disconnect(self):
        sock, self._sock = self._sock, None
        if sock is None:
            return
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    def send_object(self, payload):
        self.send_raw(json.dumps(payload, ensure_ascii=False))

    def send_raw(self, text):
        sock = self._sock
        if sock is None:
            raise ConnectionError("not connected")
        data = (text + "\n").encode("utf-8")
        with self._send_lock:
            sock.sendall(data)

    def _read_loop(self, sock):
        try:
            with sock.makefile("r", encoding="utf-8", newline="\n") as reader:
                for line in reader:
                    self._publish(line.strip())
        except (OSError, ValueError):
            pass
        finally:
            if self._sock is sock:
                self._sock = None
                self.events.put(("closed", None))
            sock.close()

    def _publish(self, line):
        if not line:
            return
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            self.events.put(("message", parsed))
        else:
            self.events.put(("raw", line))
