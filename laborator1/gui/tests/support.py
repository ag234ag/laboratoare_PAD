import tkinter as tk
import unittest

from panels import LogPanel


class FakeClient:
    def __init__(self, events=None, fail_with=None, connect_error=None):
        self.events = events
        self.fail_with = fail_with
        self.connect_error = connect_error
        self.sent = []
        self.raw = []
        self.connected = False
        self.disconnects = 0

    def connect(self, host, port):
        if self.connect_error:
            raise self.connect_error
        self.connected = True

    def disconnect(self):
        self.connected = False
        self.disconnects += 1

    def send_object(self, payload):
        if self.fail_with:
            raise self.fail_with
        self.sent.append(payload)

    def send_raw(self, text):
        if self.fail_with:
            raise self.fail_with
        self.raw.append(text)


class TkTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tk.Tk()
        self.root.withdraw()
        self.log = LogPanel(self.root)
        self.addCleanup(self._destroy_root)

    def _destroy_root(self):
        try:
            self.root.destroy()
        except tk.TclError:
            pass
