import json
import sqlite3
import tkinter as tk
import uuid
from datetime import datetime
from tkinter import ttk

from broker_state import TABLE_COLUMNS


def make_table(parent, columns, height=6):
    table = ttk.Treeview(parent, columns=columns, show="headings", height=height)
    for column in columns:
        table.heading(column, text=column)
        table.column(column, width=110, anchor="w")
    return table


class LogPanel(ttk.LabelFrame):
    def __init__(self, parent):
        super().__init__(parent, text="Log")
        self._text = tk.Text(self, height=10, state="disabled", wrap="none")
        scrollbar = ttk.Scrollbar(self, command=self._text.yview)
        self._text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        self._text.pack(side="left", fill="both", expand=True)

    def write(self, line):
        stamp = datetime.now().strftime("%H:%M:%S")
        self._text.configure(state="normal")
        self._text.insert("end", f"{stamp} {line}\n")
        self._text.see("end")
        self._text.configure(state="disabled")

    def contents(self):
        return self._text.get("1.0", "end")


class ConnectionBar(ttk.Frame):
    def __init__(self, parent, client, log, host="127.0.0.1", port="5000"):
        super().__init__(parent)
        self._client = client
        self._log = log
        self._host = tk.StringVar(value=host)
        self._port = tk.StringVar(value=port)
        self.status = tk.StringVar(value="deconectat")
        ttk.Entry(self, textvariable=self._host, width=14).pack(side="left")
        ttk.Entry(self, textvariable=self._port, width=6).pack(side="left", padx=4)
        ttk.Button(self, text="Connect", command=self.connect).pack(side="left")
        ttk.Button(self, text="Disconnect", command=self.disconnect).pack(side="left", padx=4)
        ttk.Label(self, textvariable=self.status).pack(side="left", padx=8)

    def connect(self):
        try:
            self._client.connect(self._host.get().strip(), int(self._port.get()))
        except (OSError, ValueError) as error:
            self.status.set("deconectat")
            self._log(f"ERR connect: {error}")
            return
        self.status.set("conectat")
        self._log("INFO conectat")

    def disconnect(self):
        self._client.disconnect()
        self.status.set("deconectat")

    def mark_closed(self):
        self.status.set("deconectat")


class RolePanel(ttk.LabelFrame):
    def __init__(self, parent, role, client, log):
        super().__init__(parent, text=role)
        self._role = role
        self._client = client
        self._log = log
        self.bar = ConnectionBar(self, client, lambda text: self._log.write(f"[{role}] {text}"))
        self.bar.pack(anchor="w", padx=4, pady=4)

    def _log_line(self, tag, text):
        self._log.write(f"[{self._role}] {tag} {text}")

    def _send(self, payload):
        try:
            if isinstance(payload, str):
                self._client.send_raw(payload)
            else:
                self._client.send_object(payload)
        except OSError as error:
            self._log_line("ERR", str(error))
            return False
        text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
        self._log_line(">>", text)
        return True

    def handle(self, event):
        kind, payload = event
        if kind == "closed":
            self.bar.mark_closed()
            self._log_line("!!", "conexiune inchisa")
            return
        text = json.dumps(payload, ensure_ascii=False) if kind == "message" else payload
        self._log_line("<<", text)
        if kind == "message":
            self.on_message(payload)

    def on_message(self, message):
        pass


class PublisherPanel(RolePanel):
    INVALID_JSON = '{"action":"publish","topic":"sport","content":'

    def __init__(self, parent, client, log):
        super().__init__(parent, "Publisher", client, log)
        self.topic = tk.StringVar()
        self.content = tk.StringVar()
        form = ttk.Frame(self)
        form.pack(fill="x", padx=4)
        ttk.Label(form, text="Topic").grid(row=0, column=0, sticky="w")
        ttk.Entry(form, textvariable=self.topic).grid(row=0, column=1, sticky="ew")
        ttk.Label(form, text="Message").grid(row=1, column=0, sticky="w")
        ttk.Entry(form, textvariable=self.content).grid(row=1, column=1, sticky="ew")
        form.columnconfigure(1, weight=1)
        buttons = ttk.Frame(self)
        buttons.pack(anchor="w", padx=4, pady=4)
        ttk.Button(buttons, text="Publish", command=self.publish).pack(side="left")
        ttk.Button(buttons, text="Trimite JSON invalid", command=self.send_invalid).pack(side="left", padx=4)

    def publish(self):
        topic = self.topic.get().strip()
        content = self.content.get().strip()
        if not topic or not content:
            self._log_line("ERR", "topic si mesaj obligatorii")
            return
        self._send({
            "action": "publish",
            "messageId": str(uuid.uuid4()),
            "topic": topic,
            "content": content,
        })

    def send_invalid(self):
        self._send(self.INVALID_JSON)


class SubscriberPanel(RolePanel):
    def __init__(self, parent, client, log):
        super().__init__(parent, "Subscriber", client, log)
        self.subscriber_id = tk.StringVar(value=f"tk-{uuid.uuid4().hex[:4]}")
        self.topic = tk.StringVar()
        self.auto_ack = tk.BooleanVar(value=True)
        self._rows = {}
        form = ttk.Frame(self)
        form.pack(fill="x", padx=4)
        ttk.Label(form, text="Subscriber ID").grid(row=0, column=0, sticky="w")
        ttk.Entry(form, textvariable=self.subscriber_id).grid(row=0, column=1, sticky="ew")
        ttk.Label(form, text="Topic").grid(row=1, column=0, sticky="w")
        ttk.Entry(form, textvariable=self.topic).grid(row=1, column=1, sticky="ew")
        form.columnconfigure(1, weight=1)
        buttons = ttk.Frame(self)
        buttons.pack(anchor="w", padx=4, pady=4)
        ttk.Button(buttons, text="Subscribe", command=self.subscribe).pack(side="left")
        ttk.Button(buttons, text="Unsubscribe", command=self.unsubscribe).pack(side="left", padx=4)
        ttk.Checkbutton(buttons, text="ACK automat", variable=self.auto_ack).pack(side="left", padx=4)
        ttk.Button(buttons, text="Trimite ACK", command=self.ack_selected).pack(side="left")
        self.table = make_table(self, ("messageId", "topic", "content", "attempt"))
        self.table.pack(fill="both", expand=True, padx=4, pady=4)

    def subscribe(self):
        self._topic_action("subscribe")

    def unsubscribe(self):
        self._topic_action("unsubscribe")

    def _topic_action(self, action):
        subscriber_id = self.subscriber_id.get().strip()
        topic = self.topic.get().strip()
        if not subscriber_id or not topic:
            self._log_line("ERR", "subscriber ID si topic obligatorii")
            return
        self._send({"action": action, "subscriberId": subscriber_id, "topic": topic})

    def on_message(self, message):
        if message.get("action") != "message":
            return
        message_id = message.get("messageId")
        values = (message_id, message.get("topic"), message.get("content"), message.get("attempt"))
        if message_id in self._rows:
            self.table.item(self._rows[message_id], values=values)
        else:
            self._rows[message_id] = self.table.insert("", 0, values=values)
        if self.auto_ack.get():
            self.send_ack(message_id)

    def ack_selected(self):
        selection = self.table.selection()
        if not selection:
            self._log_line("ERR", "selecteaza un mesaj")
            return
        self.send_ack(str(self.table.item(selection[0], "values")[0]))

    def send_ack(self, message_id):
        self._send({"action": "ack", "messageId": message_id})


class StatePanel(ttk.LabelFrame):
    REFRESH_MS = 1000

    def __init__(self, parent, read_state, db_path):
        super().__init__(parent, text="Stare broker (broker.db)")
        self._read_state = read_state
        self.db_path = tk.StringVar(value=db_path)
        self.status = tk.StringVar()
        self.tables = {}
        header = ttk.Frame(self)
        header.pack(fill="x", padx=4, pady=4)
        ttk.Entry(header, textvariable=self.db_path).pack(side="left", fill="x", expand=True)
        ttk.Label(header, textvariable=self.status).pack(side="left", padx=8)
        grid = ttk.Frame(self)
        grid.pack(fill="both", expand=True, padx=4, pady=4)
        for column, (name, columns) in enumerate(TABLE_COLUMNS.items()):
            ttk.Label(grid, text=name).grid(row=0, column=column, sticky="w")
            table = make_table(grid, columns, height=5)
            table.grid(row=1, column=column, sticky="nsew", padx=2)
            grid.columnconfigure(column, weight=1)
            self.tables[name] = table
        self.refresh()
        self.after(self.REFRESH_MS, self._tick)

    def refresh(self):
        try:
            state = self._read_state(self.db_path.get())
        except FileNotFoundError:
            self.status.set("broker.db negasit")
            return
        except sqlite3.Error as error:
            self.status.set(f"broker.db indisponibil: {error}")
            return
        self.status.set("ok")
        for name, rows in state.items():
            table = self.tables[name]
            table.delete(*table.get_children())
            for row in rows:
                table.insert("", "end", values=row)

    def _tick(self):
        self.refresh()
        self.after(self.REFRESH_MS, self._tick)
