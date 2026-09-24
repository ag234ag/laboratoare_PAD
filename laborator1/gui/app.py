import queue
import tkinter as tk
from pathlib import Path

from broker_client import BrokerClient
from broker_state import read_state
from panels import LogPanel, PublisherPanel, StatePanel, SubscriberPanel

POLL_MS = 100
DEFAULT_DB = str(Path(__file__).resolve().parent.parent / "broker" / "broker.db")


class App:
    def __init__(self, root, db_path=DEFAULT_DB, client_factory=BrokerClient):
        self._root = root
        root.title("Message Broker - demo")
        self._publisher_events = queue.Queue()
        self._subscriber_events = queue.Queue()
        self._publisher_client = client_factory(self._publisher_events)
        self._subscriber_client = client_factory(self._subscriber_events)
        self.log = LogPanel(root)
        self.publisher = PublisherPanel(root, self._publisher_client, self.log)
        self.subscriber = SubscriberPanel(root, self._subscriber_client, self.log)
        self.state = StatePanel(root, read_state, db_path)
        self.publisher.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
        self.subscriber.grid(row=0, column=1, sticky="nsew", padx=4, pady=4)
        self.state.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=4, pady=4)
        self.log.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=4, pady=4)
        root.columnconfigure(0, weight=1)
        root.columnconfigure(1, weight=1)
        root.rowconfigure(1, weight=1)
        root.rowconfigure(2, weight=1)
        root.protocol("WM_DELETE_WINDOW", self.close)
        root.after(POLL_MS, self.poll)

    def poll(self):
        for events, panel in (
            (self._publisher_events, self.publisher),
            (self._subscriber_events, self.subscriber),
        ):
            while True:
                try:
                    event = events.get_nowait()
                except queue.Empty:
                    break
                panel.handle(event)
        self._root.after(POLL_MS, self.poll)

    def close(self):
        self.subscriber.shutdown()
        self.publisher.bar.disconnect()
        self._root.destroy()


def main():
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
