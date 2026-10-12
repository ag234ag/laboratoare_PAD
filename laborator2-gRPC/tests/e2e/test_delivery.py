import grpc
import pytest

import oracle
from clients import BrokerApi, ListeningSubscriber, ids, new_id, wait_until
from expectations import expected_counts


def state_message(api, message_id):
    return next((m for m in api.state().messages if m.message_id == message_id), None)


def has_status(api, message_id, status):
    message = state_message(api, message_id)
    return message is not None and message.status == status


def assert_oracle_clean(broker):
    if broker.storage == "persistent":
        assert oracle.conservation_violations(broker.db_path) == []


def test_online_subscriber_receives_and_acknowledges(broker):
    with BrokerApi(broker.address) as api:
        api.subscribe("sub-1", "sport")
        subscriber = ListeningSubscriber(broker.address, "sub-1")
        try:
            message_id = new_id()
            reply = api.publish(message_id, "sport", "goal")
            assert reply.subscribers == 1

            received = subscriber.wait_for(lambda ms: message_id in ids(ms))
            assert [(m.topic, m.content, m.attempt, m.max_attempts) for m in received] == [("sport", "goal", 1, 3)]
            wait_until(lambda: has_status(api, message_id, "DELIVERED"), message="message DELIVERED")
            assert subscriber.ack_errors == []
        finally:
            subscriber.close()
    if broker.storage == "persistent":
        assert oracle.message_status(broker.db_path, message_id) == "DELIVERED"
        assert oracle.delivery(broker.db_path, message_id, "sub-1") == ("DELIVERED", 1)
        assert oracle.counts(broker.db_path) == expected_counts({"DELIVERED": 1}, {"DELIVERED": 1})
    assert_oracle_clean(broker)


def test_message_without_subscribers_is_kept_and_delivered_as_backlog(broker):
    with BrokerApi(broker.address) as api:
        message_id = new_id()
        reply = api.publish(message_id, "news", "breaking")
        assert reply.subscribers == 0
        assert has_status(api, message_id, "PENDING")

        subscriber = ListeningSubscriber(broker.address, "late-sub")
        try:
            api.subscribe("late-sub", "news")
            received = subscriber.wait_for(lambda ms: message_id in ids(ms))
            assert received[0].content == "breaking"
            wait_until(lambda: has_status(api, message_id, "DELIVERED"), message="backlog DELIVERED")
        finally:
            subscriber.close()
    if broker.storage == "persistent":
        assert oracle.counts(broker.db_path) == expected_counts({"DELIVERED": 1}, {"DELIVERED": 1})
    assert_oracle_clean(broker)


def test_missing_ack_moves_delivery_to_dead_letter_queue(broker):
    with BrokerApi(broker.address) as api:
        subscriber = ListeningSubscriber(broker.address, "lazy", auto_ack=False)
        try:
            api.subscribe("lazy", "alerts")
            message_id = new_id()
            api.publish(message_id, "alerts", "fire")

            wait_until(lambda: has_status(api, message_id, "DEAD"), timeout=60, message="message DEAD")

            attempts = [m.attempt for m in subscriber.messages() if m.message_id == message_id]
            assert attempts == [1, 2, 3]
            times = subscriber.receive_times(message_id)
            gaps = [later - earlier for earlier, later in zip(times, times[1:])]
            assert len(gaps) == 2
            assert all(4.0 <= gap <= 7.5 for gap in gaps), f"attempt gaps {gaps}"
            dead = [d for d in api.state().dead_letters if d.message_id == message_id]
            assert len(dead) == 1
            assert (dead[0].subscriber_id, dead[0].reason, dead[0].retry_count) == (
                "lazy", "ACK_NOT_RECEIVED_AFTER_MAX_RETRIES", 3)
        finally:
            subscriber.close()
    if broker.storage == "persistent":
        assert oracle.delivery(broker.db_path, message_id, "lazy") == ("DEAD", 3)
        assert oracle.counts(broker.db_path) == expected_counts({"DEAD": 1}, {"DEAD": 1}, dlq_delivery=1)
    assert_oracle_clean(broker)


def test_duplicate_ack_and_unknown_ack(broker):
    with BrokerApi(broker.address) as api:
        api.subscribe("sub-ack", "sport")
        subscriber = ListeningSubscriber(broker.address, "sub-ack")
        try:
            message_id = new_id()
            api.publish(message_id, "sport", "goal")
            wait_until(lambda: has_status(api, message_id, "DELIVERED"), message="DELIVERED")

            reply = api.ack("sub-ack", message_id)
            assert (reply.message_id, reply.duplicate) == (message_id, True)

            with pytest.raises(grpc.RpcError) as error:
                api.ack("sub-ack", new_id())
            assert error.value.code() == grpc.StatusCode.NOT_FOUND
            assert error.value.details() == "DELIVERY_NOT_FOUND"
        finally:
            subscriber.close()
    if broker.storage == "persistent":
        assert oracle.delivery(broker.db_path, message_id, "sub-ack") == ("DELIVERED", 1)
        assert oracle.counts(broker.db_path) == expected_counts({"DELIVERED": 1}, {"DELIVERED": 1})
    assert_oracle_clean(broker)


def test_reconnecting_subscriber_gets_unacked_message_again(broker):
    with BrokerApi(broker.address) as api:
        api.subscribe("flaky", "orders")
        first = ListeningSubscriber(broker.address, "flaky", auto_ack=False)
        message_id = new_id()
        try:
            api.publish(message_id, "orders", "order-1")
            first.wait_for(lambda ms: message_id in ids(ms))
        finally:
            first.close()

        second = ListeningSubscriber(broker.address, "flaky")
        try:
            received = second.wait_for(lambda ms: message_id in ids(ms), timeout=20)
            assert received[0].attempt == 2
            wait_until(lambda: has_status(api, message_id, "DELIVERED"), message="DELIVERED after reconnect")
        finally:
            second.close()
    if broker.storage == "persistent":
        assert oracle.delivery(broker.db_path, message_id, "flaky") == ("DELIVERED", 2)
        assert oracle.counts(broker.db_path) == expected_counts({"DELIVERED": 1}, {"DELIVERED": 1})
    assert_oracle_clean(broker)


def test_pending_messages_survive_broker_restart(persistent_broker):
    with BrokerApi(persistent_broker.address) as api:
        api.subscribe("offline-sub", "orders")
        message_ids = [new_id() for _ in range(3)]
        for index, message_id in enumerate(message_ids):
            api.publish(message_id, "orders", f"order-{index}")

    persistent_broker.restart()
    assert [oracle.message_status(persistent_broker.db_path, m) for m in message_ids] == ["PENDING"] * 3

    subscriber = ListeningSubscriber(persistent_broker.address, "offline-sub")
    try:
        received = subscriber.wait_for(lambda ms: set(message_ids) <= set(ids(ms)), timeout=5)
        assert ids(received)[:3] == message_ids
        wait_until(
            lambda: all(oracle.message_status(persistent_broker.db_path, m) == "DELIVERED" for m in message_ids),
            message="all DELIVERED after restart")
    finally:
        subscriber.close()
    assert oracle.counts(persistent_broker.db_path) == expected_counts({"DELIVERED": 3}, {"DELIVERED": 3})
    assert oracle.conservation_violations(persistent_broker.db_path) == []
