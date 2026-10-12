import socket
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from broker_process import BrokerProcess

ROOT = Path(__file__).resolve().parents[2]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="session")
def broker_dll():
    subprocess.run(
        ["dotnet", "build", str(ROOT / "broker" / "Broker.csproj"), "-c", "Release", "--nologo", "-v", "q"],
        check=True,
    )
    return ROOT / "broker" / "bin" / "Release" / "net9.0" / "Broker.dll"


@pytest.fixture
def make_broker(broker_dll, tmp_path):
    started = []

    def factory(storage="persistent", db_path=None, port=None):
        broker = BrokerProcess(
            broker_dll,
            storage=storage,
            db_path=db_path or tmp_path / "broker.db",
            port=port or free_port(),
            work_dir=tmp_path,
        )
        broker.start()
        started.append(broker)
        return broker

    yield factory
    failures = []
    for broker in started:
        try:
            broker.stop()
        except Exception as error:
            failures.append(error)
    if failures:
        raise failures[0]


@pytest.fixture(params=["persistent", "transient"])
def broker(request, make_broker):
    return make_broker(storage=request.param)


@pytest.fixture
def persistent_broker(make_broker):
    return make_broker(storage="persistent")


@pytest.fixture
def transient_broker(make_broker):
    return make_broker(storage="transient")
