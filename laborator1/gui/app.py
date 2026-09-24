import queue
import tkinter as tk
from pathlib import Path
from tkinter import ttk

from broker_client import BrokerClient
from broker_state import read_state
from panels import LogPanel, PublisherPanel, StatePanel, SubscriberPanel

POLL_MS = 100
SUBSCRIBER_COLUMNS = 2
DEFAULT_DB = str(Path(__file__).resolve().parent.parent / "broker" / "broker.db")


class App:
    def __init__(self, root, db_path=DEFAULT_DB, client_factory=BrokerClient):
        self._root = root
        self._client_factory = client_factory
        self._counters = {"Publisher": 0, "Subscriber": 0}
        self._events = {}
        self.publishers = []
        self.subscribers = []
        root.title("Message Broker - demo")
        self.log = LogPanel(root)
        self.state = StatePanel(root, read_state, db_path)
        toolbar = ttk.Frame(root)
        ttk.Button(toolbar, text="+ Publisher", command=self.add_publisher).pack(side="left", padx=2)
        ttk.Button(toolbar, text="+ Subscriber", command=self.add_subscriber).pack(side="left", padx=2)
        ttk.Button(toolbar, text="Connect toate", command=self.connect_all).pack(side="left", padx=2)
        self._publisher_column = ttk.Frame(root)
        self._subscriber_grid = ttk.Frame(root)
        toolbar.grid(row=0, column=0, columnspan=2, sticky="w", padx=4, pady=4)
        self._publisher_column.grid(row=1, column=0, sticky="nsew")
        self._subscriber_grid.grid(row=1, column=1, sticky="nsew")
        self.state.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=4, pady=4)
        self.log.grid(row=3, column=0, columnspan=2, sticky="nsew", padx=4, pady=4)
        root.columnconfigure(0, weight=1)
        root.columnconfigure(1, weight=3)
        root.rowconfigure(2, weight=1)
        root.rowconfigure(3, weight=1)
        for column in range(SUBSCRIBER_COLUMNS):
            self._subscriber_grid.columnconfigure(column, weight=1)
        root.protocol("WM_DELETE_WINDOW", self.close)
        self.add_publisher()
        self.add_subscriber()
        self.add_subscriber()
        root.after(POLL_MS, self.poll)

    def add_publisher(self):
        panel = self._add(PublisherPanel, "Publisher", self._publisher_column, self.publishers)
        panel.pack(fill="x", padx=4, pady=4)
        return panel

    def add_subscriber(self):
        panel = self._add(SubscriberPanel, "Subscriber", self._subscriber_grid, self.subscribers)
        self._layout_subscribers()
        return panel

    def _add(self, panel_class, role, parent, panels):
        self._counters[role] += 1
        events = queue.Queue()
        client = self._client_factory(events)
        panel = panel_class(parent, client, self.log, name=f"{role} {self._counters[role]}", on_remove=self.remove)
        self._events[panel] = events
        panels.append(panel)
        return panel

    def remove(self, panel):
        panel.shutdown()
        self._events.pop(panel, None)
        for panels in (self.publishers, self.subscribers):
            if panel in panels:
                panels.remove(panel)
        panel.destroy()
        self._layout_subscribers()

    def _layout_subscribers(self):
        for index, panel in enumerate(self.subscribers):
            panel.grid(row=index // SUBSCRIBER_COLUMNS, column=index % SUBSCRIBER_COLUMNS,
                       sticky="nsew", padx=4, pady=4)

    def connect_all(self):
        for panel in self.publishers + self.subscribers:
            panel.bar.connect()

    def poll(self):
        for panel, events in list(self._events.items()):
            while True:
                try:
                    event = events.get_nowait()
                except queue.Empty:
                    break
                panel.handle(event)
        self._root.after(POLL_MS, self.poll)

    def close(self):
        for panel in self.publishers + self.subscribers:
            panel.shutdown()
        self._root.destroy()


def main():
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
