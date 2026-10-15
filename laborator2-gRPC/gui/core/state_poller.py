import threading

from core.gateway import describe_error

STATE_KIND = "state"
STATE_ERROR_KIND = "state_error"
STATE_LIMIT = 1000


class StatePoller:
    def __init__(self, gateway, bus, interval=1.0, limit=STATE_LIMIT):
        self._gateway = gateway
        self._bus = bus
        self._interval = interval
        self._limit = limit
        self._stopped = None
        self._lock = threading.Lock()

    def start(self):
        self.stop()
        stopped = threading.Event()
        with self._lock:
            self._stopped = stopped
        threading.Thread(target=self._run, args=(stopped,), daemon=True).start()

    def stop(self):
        with self._lock:
            if self._stopped is not None:
                self._stopped.set()

    def _run(self, stopped):
        while not stopped.is_set():
            try:
                state = self._gateway.get_state(self._limit)
            except Exception as error:
                self._post_unless_stopped(stopped, STATE_ERROR_KIND, describe_error(error))
            else:
                self._post_unless_stopped(stopped, STATE_KIND, state)
            stopped.wait(self._interval)

    def _post_unless_stopped(self, stopped, kind, payload):
        with self._lock:
            if not stopped.is_set():
                self._bus.post(kind, payload)
