import importlib
import sys
from pathlib import Path

from grpc_tools import protoc

_HERE = Path(__file__).resolve().parent
PROTO_DIR = _HERE.parents[1] / "proto"
PROTO_FILE = PROTO_DIR / "broker.proto"
OUT_DIR = _HERE.parent / "generated"


def load():
    OUT_DIR.mkdir(exist_ok=True)
    generated = OUT_DIR / "broker_pb2.py"
    if not generated.exists() or generated.stat().st_mtime < PROTO_FILE.stat().st_mtime:
        exit_code = protoc.main([
            "grpc_tools.protoc",
            f"-I{PROTO_DIR}",
            f"--python_out={OUT_DIR}",
            f"--grpc_python_out={OUT_DIR}",
            str(PROTO_FILE),
        ])
        if exit_code != 0:
            raise RuntimeError(f"protoc failed for {PROTO_FILE} (exit code {exit_code})")
    if str(OUT_DIR) not in sys.path:
        sys.path.insert(0, str(OUT_DIR))
    return importlib.import_module("broker_pb2"), importlib.import_module("broker_pb2_grpc")
