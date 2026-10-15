import time
import tkinter as tk
from tkinter import ttk


class LogView(ttk.Frame):
    MAX_LINES = 2000

    def __init__(self, parent, height=8):
        super().__init__(parent)
        self._text = tk.Text(self, height=height, state="disabled", wrap="word")
        scrollbar = ttk.Scrollbar(self, command=self._text.yview)
        self._text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        self._text.pack(side="left", fill="both", expand=True)

    def append(self, line):
        self._text.configure(state="normal")
        self._text.insert("end", f"{time.strftime('%H:%M:%S')}  {line}\n")
        overflow = int(self._text.index("end-1c").split(".")[0]) - 1 - self.MAX_LINES
        if overflow > 0:
            self._text.delete("1.0", f"{overflow + 1}.0")
        self._text.configure(state="disabled")
        self._text.see("end")

    def contents(self):
        return self._text.get("1.0", "end-1c")


class TableView(ttk.Frame):
    def __init__(self, parent, columns, height=6):
        super().__init__(parent)
        self._values = {}
        names = [name for name, _ in columns]
        self._tree = ttk.Treeview(self, columns=names, show="headings", height=height, selectmode="browse")
        for name, width in columns:
            self._tree.heading(name, text=name)
            self._tree.column(name, width=width, stretch=True)
        scrollbar = ttk.Scrollbar(self, command=self._tree.yview)
        self._tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        self._tree.pack(side="left", fill="both", expand=True)

    def replace(self, rows):
        wanted = {key for key, _ in rows}
        for key in self._tree.get_children():
            if key not in wanted:
                self._tree.delete(key)
                self._values.pop(key, None)
        for index, (key, values) in enumerate(rows):
            self._values[key] = tuple(str(value) for value in values)
            if self._tree.exists(key):
                self._tree.item(key, values=self._values[key])
                self._tree.move(key, "", index)
            else:
                self._tree.insert("", index, iid=key, values=self._values[key])

    def clear(self):
        self._tree.delete(*self._tree.get_children())
        self._values.clear()

    def upsert(self, key, values):
        self._values[key] = tuple(str(value) for value in values)
        if self._tree.exists(key):
            self._tree.item(key, values=self._values[key])
        else:
            self._tree.insert("", 0, iid=key, values=self._values[key])

    def selected_key(self):
        selection = self._tree.selection()
        return selection[0] if selection else None

    def select(self, key):
        self._tree.selection_set(key)

    def rows(self):
        return [self._values[key] for key in self._tree.get_children()]


class ConnectionBar(ttk.Frame):
    def __init__(self, parent, host, port, on_connect, on_disconnect):
        super().__init__(parent)
        self.host = tk.StringVar(value=host)
        self.port = tk.StringVar(value=str(port))
        self.status = tk.StringVar(value="Deconectat")
        ttk.Label(self, text="Host").pack(side="left")
        ttk.Entry(self, textvariable=self.host, width=16).pack(side="left", padx=4)
        ttk.Label(self, text="Port").pack(side="left")
        ttk.Entry(self, textvariable=self.port, width=7).pack(side="left", padx=4)
        self.connect_button = ttk.Button(self, text="Connect", command=on_connect)
        self.connect_button.pack(side="left")
        self.disconnect_button = ttk.Button(self, text="Disconnect", command=on_disconnect)
        self.disconnect_button.pack(side="left", padx=4)
        ttk.Label(self, textvariable=self.status).pack(side="left", padx=8)
        self.set_connected(False)

    def address(self):
        host = self.host.get().strip()
        port = self.port.get().strip()
        if not host or not port.isascii() or not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError("host sau port invalid")
        return host, int(port)

    def set_connected(self, connected):
        self.connect_button.configure(state="disabled" if connected else "normal")
        self.disconnect_button.configure(state="normal" if connected else "disabled")

    def set_status(self, text):
        self.status.set(text)
