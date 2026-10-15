import importlib
import os
import sys
import tempfile
from pathlib import Path

from grpc_tools import protoc

_HERE = Path(__file__).resolve().parent
PROTO_DIR = _HERE.parents[1] / "proto"
PROTO_FILE = PROTO_DIR / "broker.proto"
OUT_DIR = _HERE.parent / "generated"
GENERATED_FILES = ("broker_pb2.py", "broker_pb2_grpc.py")


def _is_stale():
    proto_time = PROTO_FILE.stat().st_mtime
    return any(not (OUT_DIR / name).exists() or (OUT_DIR / name).stat().st_mtime < proto_time
               for name in GENERATED_FILES)


def _generate():
    with tempfile.TemporaryDirectory(prefix="generated-", dir=OUT_DIR.parent) as staging:
        exit_code = protoc.main([
            "grpc_tools.protoc",
            f"-I{PROTO_DIR}",
            f"--python_out={staging}",
            f"--grpc_python_out={staging}",
            str(PROTO_FILE),
        ])
        if exit_code != 0:
            raise RuntimeError(f"protoc failed for {PROTO_FILE} (exit code {exit_code})")
        for name in GENERATED_FILES:
            os.replace(Path(staging) / name, OUT_DIR / name)


def load():
    OUT_DIR.mkdir(exist_ok=True)
    if _is_stale():
        _generate()
    if str(OUT_DIR) not in sys.path:
        sys.path.insert(0, str(OUT_DIR))
    return importlib.import_module("broker_pb2"), importlib.import_module("broker_pb2_grpc")
