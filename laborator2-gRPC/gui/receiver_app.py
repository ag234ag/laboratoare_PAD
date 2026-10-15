import argparse
import tkinter as tk
import uuid
from tkinter import ttk

from core import publish_rules
from core.events import BackgroundCalls, EventBus, TkPump
from core.gateway import BrokerGateway, describe_error
from core.listener import Listener
from core.received_messages import ReceivedMessages
from ui.widgets import ConnectionBar, LogView, TableView

RECEIVED_COLUMNS = [("MessageId", 250), ("Topic", 90), ("Continut", 200), ("Incercare", 70), ("Stare", 190)]


class ReceiverApp:
    def __init__(self, root, host, port):
        self._root = root
        self._bus = EventBus()
        self._calls = BackgroundCalls(self._bus)
        self._gateway = None
        self._listener = None
        self._active_id = None
        self._received = ReceivedMessages()
        self._topics = set()
        root.title("Receiver gRPC")
        self.bar = ConnectionBar(root, host, port, self.connect, self.disconnect)
        self.bar.pack(fill="x", padx=6, pady=6)
        self.subscriber_id = tk.StringVar(value=f"gui-{uuid.uuid4().hex[:4]}")
        self.topic = tk.StringVar()
        self.auto_ack = tk.BooleanVar(value=True)
        self.topics_text = tk.StringVar(value="Topicuri: -")
        form = ttk.Frame(root)
        form.pack(fill="x", padx=6)
        ttk.Label(form, text="Subscriber ID").grid(row=0, column=0, sticky="w")
        ttk.Entry(form, textvariable=self.subscriber_id, width=24).grid(row=0, column=1, sticky="w", padx=4, pady=2)
        ttk.Label(form, text="Topic").grid(row=1, column=0, sticky="w")
        ttk.Entry(form, textvariable=self.topic, width=24).grid(row=1, column=1, sticky="w", padx=4, pady=2)
        buttons = ttk.Frame(root)
        buttons.pack(anchor="w", padx=6, pady=6)
        self._actions = [
            ttk.Button(buttons, text="Subscribe", command=self.subscribe),
            ttk.Button(buttons, text="Unsubscribe", command=self.unsubscribe),
            ttk.Button(buttons, text="Trimite ACK", command=self.ack_selected),
        ]
        for button in self._actions:
            button.pack(side="left", padx=2)
        ttk.Checkbutton(buttons, text="ACK automat", variable=self.auto_ack).pack(side="left", padx=8)
        ttk.Label(root, textvariable=self.topics_text).pack(anchor="w", padx=6)
        self.table = TableView(root, RECEIVED_COLUMNS, height=10)
        self.table.pack(fill="both", expand=True, padx=6, pady=4)
        self.log = LogView(root, height=6)
        self.log.pack(fill="x", padx=6, pady=4)
        self._set_actions(False)
        TkPump(root, self._bus, self._handle).start()
        root.protocol("WM_DELETE_WINDOW", self.close)

    def connect(self):
        subscriber_id = self.subscriber_id.get().strip()
        error = publish_rules.validate_subscriber_id(subscriber_id)
        if error:
            self.log.append(f"Eroare: {error}.")
            return
        try:
            host, port = self.bar.address()
        except ValueError as error:
            self.log.append(str(error))
            return
        self._drop_connection()
        gateway = BrokerGateway(host, port)
        self._gateway = gateway
        self._active_id = subscriber_id
        self.bar.set_status("Se conecteaza...")
        self._calls.submit("connect", gateway.ping, context=gateway)

    def disconnect(self):
        self._drop_connection()
        self.bar.set_connected(False)
        self.bar.set_status("Deconectat")
        self._set_actions(False)

    def close(self):
        self._drop_connection()
        self._root.destroy()

    def subscribe(self):
        self._topic_action("subscribe")

    def unsubscribe(self):
        self._topic_action("unsubscribe")

    def ack_selected(self):
        message_id = self.table.selected_key()
        if message_id is None:
            self.log.append("Selecteaza un mesaj din tabel.")
            return
        self._send_ack(message_id)

    def _topic_action(self, name):
        topic = self.topic.get().strip()
        error = publish_rules.validate_topic(topic)
        if error:
            self.log.append(f"Eroare: {error}.")
            return
        if self._gateway is None:
            self.log.append("Nu esti conectat.")
            return
        self._calls.submit("subscription", getattr(self._gateway, name), self._active_id, topic,
                           context=(name, topic))

    def _send_ack(self, message_id):
        if self._gateway is None:
            self.log.append("Nu esti conectat.")
            return
        self._calls.submit("ack", self._gateway.ack, self._active_id, message_id, context=message_id)

    def _handle(self, kind, payload):
        handlers = {
            "connect": self._on_connect,
            "message": self._on_message,
            "listen_closed": self._on_listen_closed,
            "subscription": self._on_subscription,
            "ack": self._on_ack,
        }
        handlers[kind](payload)

    def _on_connect(self, outcome):
        if outcome.context is not self._gateway:
            outcome.context.close()
            return
        if not outcome.ok:
            self.log.append(describe_error(outcome.error))
            self._drop_connection()
            self.bar.set_connected(False)
            self.bar.set_status("Deconectat")
            return
        self._listener = Listener(self._gateway, self._bus)
        self._listener.start(self._active_id)
        self.bar.set_connected(True)
        self.bar.set_status(f"Listen ca '{self._active_id}'")
        self._set_actions(True)
        self.log.append(f"Listen deschis ca '{self._active_id}'")

    def _on_message(self, delivered):
        item = self._received.register(delivered)
        self.table.upsert(item.message_id, item.row())
        self.log.append(f"[PRIMIT] {item.message_id} topic={item.topic} incercarea {item.attempt}/{item.max_attempts}")
        if self.auto_ack.get():
            self._send_ack(item.message_id)

    def _on_listen_closed(self, closed):
        self.log.append(closed.text())
        self._drop_connection()
        self.bar.set_connected(False)
        self.bar.set_status("Deconectat")
        self._set_actions(False)

    def _on_subscription(self, outcome):
        name, topic = outcome.context
        if not outcome.ok:
            self.log.append(f"{name} esuat pentru '{topic}': {describe_error(outcome.error)}")
            return
        if name == "subscribe":
            self._topics.add(topic)
        else:
            self._topics.discard(topic)
        self.topics_text.set(f"Topicuri: {', '.join(sorted(self._topics)) or '-'}")
        self.log.append(f"[BROKER] {name} confirmat: {topic}")

    def _on_ack(self, outcome):
        message_id = outcome.context
        if not outcome.ok:
            self.log.append(f"ACK esuat pentru {message_id}: {describe_error(outcome.error)}")
            return
        item = self._received.mark_acked(message_id, outcome.value.duplicate)
        if item is not None:
            self.table.upsert(item.message_id, item.row())
        self.log.append(f"[ACK] {message_id}{' (duplicat)' if outcome.value.duplicate else ''}")

    def _drop_connection(self):
        listener, self._listener = self._listener, None
        if listener is not None:
            listener.stop()
        gateway, self._gateway = self._gateway, None
        if gateway is not None:
            gateway.close()

    def _set_actions(self, enabled):
        for button in self._actions:
            button.configure(state="normal" if enabled else "disabled")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Receiver GUI (gRPC)")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host/IP (implicit 127.0.0.1)")
    parser.add_argument("--port", type=int, default=50051, help="Broker port (implicit 50051)")
    return parser.parse_args(argv)


def main():
    args = parse_args()
    root = tk.Tk()
    ReceiverApp(root, args.host, args.port)
    root.mainloop()


if __name__ == "__main__":
    main()
