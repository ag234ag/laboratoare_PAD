import queue
import threading
from dataclasses import dataclass


@dataclass(frozen=True)
class Outcome:
    context: object
    value: object = None
    error: Exception | None = None

    @property
    def ok(self):
        return self.error is None


class EventBus:
    def __init__(self):
        self._queue = queue.Queue()

    def post(self, kind, payload=None):
        self._queue.put((kind, payload))

    def drain(self):
        items = []
        while True:
            try:
                items.append(self._queue.get_nowait())
            except queue.Empty:
                return items


class BackgroundCalls:
    def __init__(self, bus):
        self._bus = bus

    def submit(self, kind, function, *args, context=None):
        threading.Thread(target=self._run, args=(kind, function, args, context), daemon=True).start()

    def _run(self, kind, function, args, context):
        try:
            outcome = Outcome(context, value=function(*args))
        except Exception as error:
            outcome = Outcome(context, error=error)
        self._bus.post(kind, outcome)


class TkPump:
    def __init__(self, root, bus, handler, interval_ms=100):
        self._root = root
        self._bus = bus
        self._handler = handler
        self._interval_ms = interval_ms

    def start(self):
        self._tick()

    def _tick(self):
        try:
            for kind, payload in self._bus.drain():
                self._handler(kind, payload)
        finally:
            self._root.after(self._interval_ms, self._tick)
