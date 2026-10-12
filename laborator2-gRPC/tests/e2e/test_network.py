import socket

import grpc
import pytest

import oracle
from clients import BrokerApi, ListeningSubscriber, ids, new_id, wait_until
from expectations import expected_counts, wait_for_db_status


def lan_ipv4():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        try:
            sock.connect(("10.255.255.255", 1))
            address = sock.getsockname()[0]
        except OSError:
            return None
    return None if address.startswith("127.") else address


def test_clients_reach_broker_through_lan_address(persistent_broker):
    ip = lan_ipv4()
    if ip is None:
        pytest.skip("no LAN IPv4 address on this machine")
    address = f"{ip}:{persistent_broker.port}"

    with BrokerApi(address) as api:
        api.subscribe("lan-sub", "lan")
        subscriber = ListeningSubscriber(address, "lan-sub")
        try:
            message_id = new_id()
            api.publish(message_id, "lan", "over the network")
            received = subscriber.wait_for(lambda ms: message_id in ids(ms))
            assert received[0].content == "over the network"
            wait_for_db_status(persistent_broker.db_path, message_id, "DELIVERED")
        finally:
            subscriber.close()

    log = persistent_broker.log_text()
    assert f"connect to {ip}:{persistent_broker.port}" in log
    assert ip in log.split("[ONLINE] lan-sub from", 1)[1].splitlines()[0]
    assert oracle.counts(persistent_broker.db_path) == expected_counts({"DELIVERED": 1}, {"DELIVERED": 1})
    assert oracle.conservation_violations(persistent_broker.db_path) == []


def test_second_connection_with_same_id_replaces_first(persistent_broker):
    with BrokerApi(persistent_broker.address) as api:
        api.subscribe("dup-id", "dup")
        first = ListeningSubscriber(persistent_broker.address, "dup-id")
        wait_until(lambda: "dup-id" in api.state().online_subscribers, message="first online")

        second = ListeningSubscriber(persistent_broker.address, "dup-id")
        try:
            error = first.wait_until_closed(10)
            assert error is not None
            assert error.code() == grpc.StatusCode.ABORTED
            assert error.details() == "SESSION_REPLACED"

            message_id = new_id()
            api.publish(message_id, "dup", "for the newest connection")
            second.wait_for(lambda ms: message_id in ids(ms))
            assert message_id not in ids(first.messages())
            wait_for_db_status(persistent_broker.db_path, message_id, "DELIVERED")
        finally:
            second.close()
            first.close()
    assert oracle.counts(persistent_broker.db_path) == expected_counts({"DELIVERED": 1}, {"DELIVERED": 1})
    assert oracle.conservation_violations(persistent_broker.db_path) == []
