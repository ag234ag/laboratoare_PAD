import argparse
import time
import tkinter as tk
import uuid
from tkinter import ttk

from core import publish_rules
from core.events import BackgroundCalls, EventBus, TkPump
from core.gateway import BrokerGateway, describe_error
from ui.widgets import ConnectionBar, LogView, TableView

HISTORY_COLUMNS = [("Ora", 70), ("MessageId", 270), ("Topic", 110), ("Rezultat", 170)]


class PublisherApp:
    def __init__(self, root, host, port):
        self._root = root
        self._bus = EventBus()
        self._calls = BackgroundCalls(self._bus)
        self._gateway = None
        self._last = None
        self._row = 0
        root.title("Publisher gRPC")
        self.bar = ConnectionBar(root, host, port, self.connect, self.disconnect)
        self.bar.pack(fill="x", padx=6, pady=6)
        self.topic = tk.StringVar()
        self.content = tk.StringVar()
        form = ttk.Frame(root)
        form.pack(fill="x", padx=6)
        form.columnconfigure(1, weight=1)
        ttk.Label(form, text="Topic").grid(row=0, column=0, sticky="w")
        ttk.Entry(form, textvariable=self.topic).grid(row=0, column=1, sticky="ew", padx=4, pady=2)
        ttk.Label(form, text="Message").grid(row=1, column=0, sticky="w")
        ttk.Entry(form, textvariable=self.content).grid(row=1, column=1, sticky="ew", padx=4, pady=2)
        buttons = ttk.Frame(root)
        buttons.pack(anchor="w", padx=6, pady=6)
        self._actions = [
            ttk.Button(buttons, text="Publish", command=self.publish),
            ttk.Button(buttons, text="Fara topic (DLQ)", command=self.publish_without_topic),
            ttk.Button(buttons, text="Fara continut (DLQ)", command=self.publish_without_content),
            ttk.Button(buttons, text="Retrimite ultimul", command=self.resend_last),
            ttk.Button(buttons, text="Ping", command=self.ping),
        ]
        for button in self._actions:
            button.pack(side="left", padx=2)
        self.history = TableView(root, HISTORY_COLUMNS, height=10)
        self.history.pack(fill="both", expand=True, padx=6, pady=4)
        self.log = LogView(root, height=6)
        self.log.pack(fill="x", padx=6, pady=4)
        self._set_actions(False)
        TkPump(root, self._bus, self._handle).start()
        root.protocol("WM_DELETE_WINDOW", self.close)

    def connect(self):
        try:
            host, port = self.bar.address()
        except ValueError as error:
            self.log.append(str(error))
            return
        self._drop_gateway()
        gateway = BrokerGateway(host, port)
        self._gateway = gateway
        self.bar.set_status("Se conecteaza...")
        self._calls.submit("connect", gateway.ping, context=gateway)

    def disconnect(self):
        self._drop_gateway()
        self.bar.set_connected(False)
        self.bar.set_status("Deconectat")
        self._set_actions(False)

    def close(self):
        self._drop_gateway()
        self._root.destroy()

    def publish(self):
        topic, content = self.topic.get().strip(), self.content.get().strip()
        error = publish_rules.validate_topic(topic) or publish_rules.validate_content(content)
        if error:
            self.log.append(f"Eroare: {error}.")
            return
        message = (str(uuid.uuid4()), topic, content)
        if self._send(*message):
            self._last = message

    def publish_without_topic(self):
        self._send(str(uuid.uuid4()), "", "message without topic")

    def publish_without_content(self):
        self._send(str(uuid.uuid4()), "dlq-test", "")

    def resend_last(self):
        if self._last is None:
            self.log.append("Publica un mesaj mai intai.")
            return
        self._send(*self._last)

    def ping(self):
        if self._gateway is None:
            self.log.append("Nu esti conectat.")
            return
        self._calls.submit("ping", self._gateway.ping)

    def _send(self, message_id, topic, content):
        if self._gateway is None:
            self.log.append("Nu esti conectat.")
            return False
        self._calls.submit("publish", self._gateway.publish, message_id, topic, content,
                           context=(message_id, topic))
        return True

    def _handle(self, kind, payload):
        {"connect": self._on_connect, "publish": self._on_publish, "ping": self._on_ping}[kind](payload)

    def _on_connect(self, outcome):
        if outcome.context is not self._gateway:
            outcome.context.close()
            return
        if not outcome.ok:
            self.log.append(describe_error(outcome.error))
            self._drop_gateway()
            self.bar.set_connected(False)
            self.bar.set_status("Deconectat")
            return
        self.bar.set_connected(True)
        self.bar.set_status(f"Conectat, storage {outcome.value.storage_mode}")
        self._set_actions(True)

    def _on_publish(self, outcome):
        message_id, topic = outcome.context
        label = publish_rules.publish_label(outcome.value, outcome.error)
        self._row += 1
        self.history.upsert(f"r{self._row}", (time.strftime("%H:%M:%S"), message_id, topic, label))
        self.log.append(f"{message_id} topic={topic!r}: {label}")

    def _on_ping(self, outcome):
        if outcome.ok:
            self.log.append(f"PONG - ora brokerului {outcome.value.server_time_utc}, storage {outcome.value.storage_mode}")
        else:
            self.log.append(describe_error(outcome.error))

    def _drop_gateway(self):
        gateway, self._gateway = self._gateway, None
        if gateway is not None:
            gateway.close()

    def _set_actions(self, enabled):
        for button in self._actions:
            button.configure(state="normal" if enabled else "disabled")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Publisher GUI (gRPC)")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host/IP (implicit 127.0.0.1)")
    parser.add_argument("--port", type=int, default=50051, help="Broker port (implicit 50051)")
    return parser.parse_args(argv)


def main():
    args = parse_args()
    root = tk.Tk()
    PublisherApp(root, args.host, args.port)
    root.mainloop()


if __name__ == "__main__":
    main()
