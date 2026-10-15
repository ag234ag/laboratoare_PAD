import unittest

from core import stubs


class StubsTest(unittest.TestCase):
    def test_load_returns_generated_modules(self):
        pb2, pb2_grpc = stubs.load()
        request = pb2.PublishRequest(message_id="m1", topic="t", content="c")
        self.assertEqual("t", request.topic)
        self.assertTrue(hasattr(pb2_grpc, "MessageBrokerStub"))

    def test_generated_files_live_inside_gui(self):
        self.assertEqual("generated", stubs.OUT_DIR.name)
        self.assertEqual("gui", stubs.OUT_DIR.parent.name)


if __name__ == "__main__":
    unittest.main()
