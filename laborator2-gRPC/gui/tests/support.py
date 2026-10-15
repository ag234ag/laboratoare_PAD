import socket
import time
import tkinter as tk

import grpc


class FakeRpcError(grpc.RpcError):
    def __init__(self, code, details=""):
        super().__init__()
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


def unused_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def make_root(test_case):
    try:
        root = tk.Tk()
    except tk.TclError as error:
        test_case.skipTest(f"fara display: {error}")
    root.withdraw()

    def destroy():
        try:
            root.destroy()
        except tk.TclError:
            pass

    test_case.addCleanup(destroy)
    return root


def pump(root, condition, timeout=5, message="conditia"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        root.update()
        if condition():
            return
        time.sleep(0.02)
    raise AssertionError(f"{message} nu s-a indeplinit in {timeout}s")
