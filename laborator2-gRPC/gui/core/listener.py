import threading
from dataclasses import dataclass

import grpc

from core.gateway import UNAVAILABLE_TEXT

MESSAGE_KIND = "message"
CLOSED_KIND = "listen_closed"
REPLACED_CODE = "ABORTED"
REPLACED_DETAILS = "SESSION_REPLACED"


@dataclass(frozen=True)
class ListenClosed:
    code: str
    details: str

    @property
    def replaced(self):
        return self.code == REPLACED_CODE and self.details == REPLACED_DETAILS

    def text(self):
        if self.replaced:
            return "Sesiunea a fost inlocuita de alt receiver cu acelasi ID."
        if self.code == "UNAVAILABLE":
            return UNAVAILABLE_TEXT
        return f"Listen inchis: {self.code} - {self.details}"


class Listener:
    def __init__(self, gateway, bus):
        self._gateway = gateway
        self._bus = bus
        self._call = None
        self._lock = threading.Lock()

    def start(self, subscriber_id):
        self.stop()
        call = self._gateway.listen(subscriber_id)
        with self._lock:
            self._call = call
        threading.Thread(target=self._run, args=(call,), daemon=True).start()

    def stop(self):
        with self._lock:
            call, self._call = self._call, None
        if call is not None:
            call.cancel()

    def _run(self, call):
        try:
            for message in call:
                if not self._post_if_current(call, MESSAGE_KIND, message):
                    return
            closed = ListenClosed("OK", "stream inchis de broker")
        except grpc.RpcError as error:
            closed = ListenClosed(error.code().name, error.details() or "")
        except Exception as error:
            closed = ListenClosed("INTERNAL", str(error))
        with self._lock:
            if self._call is call:
                self._call = None
                self._bus.post(CLOSED_KIND, closed)

    def _post_if_current(self, call, kind, payload):
        with self._lock:
            if self._call is not call:
                return False
            self._bus.post(kind, payload)
            return True
