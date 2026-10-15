import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

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


class GenerationTest(unittest.TestCase):
    def setUp(self):
        workspace = tempfile.TemporaryDirectory()
        self.addCleanup(workspace.cleanup)
        self.workspace = Path(workspace.name)
        self.out_dir = self.workspace / "generated"
        for patcher in (mock.patch.object(stubs, "OUT_DIR", self.out_dir),
                        mock.patch.object(sys, "path", list(sys.path))):
            patcher.start()
            self.addCleanup(patcher.stop)
        stubs.load()
        self.pb2 = self.out_dir / "broker_pb2.py"
        self.pb2_grpc = self.out_dir / "broker_pb2_grpc.py"

    def test_first_load_creates_both_files_and_leaves_no_staging_directory(self):
        self.assertTrue(self.pb2.exists())
        self.assertTrue(self.pb2_grpc.exists())
        self.assertEqual(["generated"], [entry.name for entry in self.workspace.iterdir()])

    def test_up_to_date_files_are_not_regenerated(self):
        before = (self.pb2.stat().st_mtime_ns, self.pb2_grpc.stat().st_mtime_ns)
        stubs.load()
        self.assertEqual(before, (self.pb2.stat().st_mtime_ns, self.pb2_grpc.stat().st_mtime_ns))

    def test_missing_grpc_file_is_regenerated(self):
        self.pb2_grpc.unlink()
        stubs.load()
        self.assertTrue(self.pb2_grpc.exists())

    def test_missing_pb2_file_is_regenerated(self):
        self.pb2.unlink()
        stubs.load()
        self.assertTrue(self.pb2.exists())

    def test_grpc_file_older_than_the_proto_is_regenerated(self):
        os.utime(self.pb2_grpc, (0, 0))
        stubs.load()
        self.assertGreater(self.pb2_grpc.stat().st_mtime, stubs.PROTO_FILE.stat().st_mtime)

    def test_protoc_failure_raises_and_publishes_nothing(self):
        self.pb2_grpc.unlink()
        self.pb2.unlink()
        with mock.patch.object(stubs.protoc, "main", return_value=1):
            with self.assertRaises(RuntimeError):
                stubs.load()
        self.assertEqual([], list(self.out_dir.iterdir()))
        self.assertEqual(["generated"], [entry.name for entry in self.workspace.iterdir()])


if __name__ == "__main__":
    unittest.main()
