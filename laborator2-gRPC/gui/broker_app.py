import tkinter as tk
from pathlib import Path
from tkinter import ttk

from core import state_view
from core.broker_options import STORAGE_MODES, BrokerOptions
from core.broker_process import BrokerProcess, PortInUseError, wait_until_ready
from core.events import BackgroundCalls, EventBus, TaggedBus, TkPump
from core.gateway import BrokerGateway
from core.state_poller import StatePoller
from ui.widgets import LogView, TableView

STARTING = "Porneste..."
FAILED = "Esuat"


class BrokerApp:
    def __init__(self, root, broker_project, work_dir, process_factory=None):
        self._root = root
        self._bus = EventBus()
        self._calls = BackgroundCalls(self._bus)
        factory = process_factory or (lambda bus: BrokerProcess(bus, broker_project, work_dir))
        self._process = factory(self._bus)
        self._gateway = None
        self._poller = None
        self._last_state_error = None
        root.title("Broker gRPC")
        self.port = tk.StringVar(value="50051")
        self.storage = tk.StringVar(value="persistent")
        self.db = tk.StringVar(value="broker.db")
        self.workers = tk.StringVar(value="4")
        self.status = tk.StringVar(value="Oprit")
        self.online = tk.StringVar(value="Abonati online: -")
        options = ttk.Frame(root)
        options.pack(fill="x", padx=6, pady=6)
        self._inputs = []
        for label, widget in (
            ("Port", ttk.Entry(options, textvariable=self.port, width=7)),
            ("Storage", ttk.Combobox(options, textvariable=self.storage, values=STORAGE_MODES, width=11, state="readonly")),
            ("Db", ttk.Entry(options, textvariable=self.db, width=18)),
            ("Workers", ttk.Entry(options, textvariable=self.workers, width=4)),
        ):
            ttk.Label(options, text=label).pack(side="left")
            widget.pack(side="left", padx=4)
            self._inputs.append(widget)
        self.start_button = ttk.Button(options, text="Start", command=self.start)
        self.start_button.pack(side="left", padx=4)
        self.stop_button = ttk.Button(options, text="Stop", command=self.stop)
        self.stop_button.pack(side="left")
        ttk.Label(options, textvariable=self.status).pack(side="left", padx=10)
        ttk.Label(root, textvariable=self.online).pack(anchor="w", padx=6)
        notebook = ttk.Notebook(root)
        notebook.pack(fill="both", expand=True, padx=6, pady=4)
        self.messages_table = self._tab(notebook, "Messages", state_view.MESSAGE_COLUMNS)
        self.deliveries_table = self._tab(notebook, "Deliveries", state_view.DELIVERY_COLUMNS)
        self.dead_letters_table = self._tab(notebook, "DeadLetters", state_view.DEAD_LETTER_COLUMNS)
        self.subscriptions_table = self._tab(notebook, "Subscriptions", state_view.SUBSCRIPTION_COLUMNS)
        self.console = LogView(root, height=12)
        self.console.pack(fill="both", expand=True, padx=6, pady=4)
        self._set_controls(running=False)
        TkPump(root, self._bus, self._handle).start()
        root.protocol("WM_DELETE_WINDOW", self.close)

    @staticmethod
    def _tab(notebook, title, columns):
        table = TableView(notebook, columns, height=8)
        notebook.add(table, text=title)
        return table

    def start(self):
        if self._process.running:
            self.console.append("Brokerul ruleaza deja.")
            return
        options = BrokerOptions(self.port.get().strip(), self.storage.get(), self.db.get().strip(), self.workers.get().strip())
        problems = options.errors()
        if problems:
            for problem in problems:
                self.console.append(f"Eroare: {problem}.")
            return
        try:
            self._process.start(options)
        except PortInUseError as error:
            self.console.append(str(error))
            return
        except OSError as error:
            self.console.append(f"Nu pot porni brokerul (este dotnet instalat?): {error}")
            return
        self._stop_polling()
        self._gateway = BrokerGateway("127.0.0.1", int(options.port))
        self.status.set(STARTING)
        self._set_controls(running=True)
        self._calls.submit("ready", wait_until_ready, self._gateway.ping, lambda: self._process.running,
                           context=self._gateway)

    def stop(self):
        self._halt_poller()
        self._stop_process()

    def close(self):
        self._stop_polling()
        self._stop_process()
        self._root.destroy()

    def _stop_process(self):
        try:
            self._process.stop()
        except RuntimeError as error:
            self.console.append(f"Eroare la oprire: {error}")

    def _handle(self, kind, payload):
        handlers = {
            "console": self.console.append,
            "exited": self._on_exited,
            "ready": self._on_ready,
            "state": self._on_state,
            "state_error": self._on_state_error,
        }
        handlers[kind](payload)

    def _on_ready(self, outcome):
        if outcome.context is not self._gateway:
            return
        if outcome.ok and outcome.value:
            self.status.set("Running")
            self._poller = StatePoller(self._gateway, TaggedBus(self._bus, self._gateway))
            self._poller.start()
            return
        self.status.set(FAILED)
        self.console.append("Brokerul nu a raspuns la Ping.")
        self._stop_process()

    def _on_exited(self, code):
        self._stop_polling()
        failed = self.status.get() in (STARTING, FAILED)
        self.status.set(f"{FAILED} (cod {code})" if failed else f"Oprit (cod {code})")
        self.console.append(f"Procesul brokerului s-a oprit (cod {code}).")
        self._set_controls(running=False)

    def _on_state(self, payload):
        tag, state = payload
        if tag is not self._gateway or self._poller is None:
            return
        self._last_state_error = None
        self.messages_table.replace(state_view.message_rows(state))
        self.deliveries_table.replace(state_view.delivery_rows(state))
        self.dead_letters_table.replace(state_view.dead_letter_rows(state))
        self.subscriptions_table.replace(state_view.subscription_rows(state))
        self.online.set(state_view.online_text(state))

    def _on_state_error(self, payload):
        tag, message = payload
        if tag is not self._gateway or self._poller is None or message == self._last_state_error:
            return
        self._last_state_error = message
        self.console.append(f"Stare: {message}")

    def _halt_poller(self):
        poller, self._poller = self._poller, None
        if poller is not None:
            poller.stop()

    def _stop_polling(self):
        self._halt_poller()
        gateway, self._gateway = self._gateway, None
        if gateway is not None:
            gateway.close()

    def _set_controls(self, running):
        self.start_button.configure(state="disabled" if running else "normal")
        self.stop_button.configure(state="normal" if running else "disabled")
        for widget in self._inputs:
            widget.configure(state="disabled" if running else ("readonly" if isinstance(widget, ttk.Combobox) else "normal"))


def main():
    root = tk.Tk()
    lab_dir = Path(__file__).resolve().parents[1]
    BrokerApp(root, lab_dir / "broker", lab_dir)
    root.mainloop()


if __name__ == "__main__":
    main()
