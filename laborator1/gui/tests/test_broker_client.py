import queue
import socket
import threading
import unittest

from broker_client import BrokerClient


class EchoServer:
    def __init__(self, reply=None):
        self._listener = socket.socket()
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen()
        self.port = self._listener.getsockname()[1]
        self.received = queue.Queue()
        self.accepted = []
        self._reply = reply
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while True:
            try:
                conn, _ = self._listener.accept()
            except OSError:
                return
            self.accepted.append(conn)
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        try:
            with conn.makefile("rb") as reader:
                for line in reader:
                    self.received.put(line.decode("utf-8"))
                    out = self._reply(line) if self._reply else line
                    if out is not None:
                        conn.sendall(out)
        except OSError:
            pass

    def close_clients(self):
        for conn in self.accepted:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()

    def stop(self):
        self._listener.close()
        self.close_clients()


class BrokerClientTest(unittest.TestCase):
    def setUp(self):
        self.events = queue.Queue()
        self.client = BrokerClient(self.events)
        self.server = None
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        self.client.disconnect()
        if self.server:
            self.server.stop()

    def start_server(self, reply=None):
        self.server = EchoServer(reply)
        return self.server

    def test_sends_json_line_and_parses_reply(self):
        server = self.start_server(lambda line: b'{"action":"pong"}\n')
        self.client.connect("127.0.0.1", server.port)
        self.client.send_object({"action": "ping"})
        self.assertEqual(server.received.get(timeout=2), '{"action": "ping"}\n')
        self.assertEqual(self.events.get(timeout=2), ("message", {"action": "pong"}))

    def test_unicode_quotes_and_backslash_survive_round_trip(self):
        content = 'ăîșț 😀 "q" \\ x {b}'
        server = self.start_server()
        self.client.connect("127.0.0.1", server.port)
        self.client.send_object({"action": "publish", "content": content})
        kind, payload = self.events.get(timeout=2)
        self.assertEqual(kind, "message")
        self.assertEqual(payload["content"], content)

    def test_text_line_is_delivered_raw(self):
        server = self.start_server(lambda line: b"not json\n")
        self.client.connect("127.0.0.1", server.port)
        self.client.send_raw("hello")
        self.assertEqual(self.events.get(timeout=2), ("raw", "not json"))

    def test_json_array_is_delivered_raw_and_reader_survives(self):
        replies = iter([b"[1,2]\n", b'{"action":"pong"}\n'])
        server = self.start_server(lambda line: next(replies))
        self.client.connect("127.0.0.1", server.port)
        self.client.send_raw("a")
        self.client.send_raw("b")
        self.assertEqual(self.events.get(timeout=2), ("raw", "[1,2]"))
        self.assertEqual(self.events.get(timeout=2), ("message", {"action": "pong"}))

    def test_server_closing_emits_closed_and_marks_disconnected(self):
        server = self.start_server(lambda line: None)
        self.client.connect("127.0.0.1", server.port)
        self.client.send_raw("x")
        server.received.get(timeout=2)
        server.close_clients()
        self.assertEqual(self.events.get(timeout=2), ("closed", None))
        self.assertFalse(self.client.connected)
        with self.assertRaises(OSError):
            self.client.send_raw("again")

    def test_send_when_not_connected_raises_connection_error(self):
        with self.assertRaises(ConnectionError):
            self.client.send_object({"action": "ping"})

    def test_connect_to_closed_port_raises_os_error(self):
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        with self.assertRaises(OSError):
            self.client.connect("127.0.0.1", port)
        self.assertFalse(self.client.connected)

    def test_second_connect_replaces_first_without_false_closed_event(self):
        server = self.start_server(lambda line: None)
        self.client.connect("127.0.0.1", server.port)
        self.client.connect("127.0.0.1", server.port)
        self.client.send_raw("ping")
        server.received.get(timeout=2)
        with self.assertRaises(queue.Empty):
            self.events.get(timeout=0.5)
        self.assertTrue(self.client.connected)

    def test_disconnect_is_silent_and_idempotent(self):
        server = self.start_server(lambda line: None)
        self.client.connect("127.0.0.1", server.port)
        self.client.disconnect()
        self.client.disconnect()
        with self.assertRaises(queue.Empty):
            self.events.get(timeout=0.5)
        self.assertFalse(self.client.connected)


if __name__ == "__main__":
    unittest.main()
