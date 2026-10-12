import grpc
import pytest

import oracle
from clients import BrokerApi, broker_pb2, new_id
from expectations import expected_counts


@pytest.mark.parametrize(
    "message_id, topic, content, expected",
    [
        ("", "sport", "text", "MESSAGE_ID_REQUIRED"),
        (None, "", "text", "TOPIC_REQUIRED"),
        (None, "sport", "   ", "CONTENT_REQUIRED"),
        (None, "bad topic!", "text", "TOPIC_INVALID_CHARS"),
        (None, "t" * 65, "text", "TOPIC_TOO_LONG"),
        (None, "sport", "x" * 4097, "CONTENT_TOO_LONG"),
        (None, "", "", "TOPIC_REQUIRED,CONTENT_REQUIRED"),
    ],
)
def test_invalid_publish_is_rejected_and_dead_lettered(broker, message_id, topic, content, expected):
    with BrokerApi(broker.address) as api:
        with pytest.raises(grpc.RpcError) as error:
            api.publish(new_id() if message_id is None else message_id, topic, content)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert error.value.details() == expected

        state = api.state()
        assert [(d.message_id, d.reason) for d in state.dead_letters] == [("", expected)]
        assert len(state.messages) == 0

    if broker.storage == "persistent":
        rows = oracle.dead_letters(broker.db_path)
        assert len(rows) == 1
        stored_message_id, subscriber_id, _, raw_payload, reason, retry_count = rows[0]
        assert (stored_message_id, subscriber_id, reason, retry_count) == (None, None, expected, 0)
        assert '"topic"' in raw_payload and '"content"' in raw_payload
        assert oracle.counts(broker.db_path) == expected_counts(dlq_invalid=1)
        assert oracle.conservation_violations(broker.db_path) == []


def test_duplicate_message_id_is_rejected_without_overwriting(broker):
    with BrokerApi(broker.address) as api:
        message_id = new_id()
        api.publish(message_id, "sport", "first")
        with pytest.raises(grpc.RpcError) as error:
            api.publish(message_id, "sport", "second")
        assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS
        assert error.value.details() == "DUPLICATE_MESSAGE_ID"

        stored = [m for m in api.state().messages if m.message_id == message_id]
        assert [m.content for m in stored] == ["first"]
        assert len(api.state().dead_letters) == 0
    if broker.storage == "persistent":
        assert [row[2] for row in oracle.messages(broker.db_path)] == ["first"]
        assert oracle.counts(broker.db_path) == expected_counts({"PENDING": 1})
        assert oracle.conservation_violations(broker.db_path) == []


def test_invalid_subscribe_is_rejected_without_dead_letter(broker):
    with BrokerApi(broker.address) as api:
        with pytest.raises(grpc.RpcError) as error:
            api.subscribe("java pc", "sport")
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert error.value.details() == "SUBSCRIBER_ID_INVALID_CHARS"
        assert len(api.state().dead_letters) == 0
        assert len(api.state().subscriptions) == 0
    if broker.storage == "persistent":
        assert oracle.counts(broker.db_path) == expected_counts()
        assert oracle.conservation_violations(broker.db_path) == []


def test_corrupted_and_oversized_payloads_do_not_crash_broker(persistent_broker):
    with grpc.insecure_channel(persistent_broker.address) as channel:
        raw_publish = channel.unary_unary("/pad.broker.v1.MessageBroker/Publish")

        with pytest.raises(grpc.RpcError) as corrupted:
            raw_publish(b"\x0a\x05ab", timeout=5)
        assert corrupted.value.code() not in (grpc.StatusCode.UNIMPLEMENTED, grpc.StatusCode.UNAVAILABLE)
        assert "InvalidProtocolBufferException" in persistent_broker.log_text()

        oversized = broker_pb2.PublishRequest(message_id=new_id(), topic="sport", content="x" * 70000)
        with pytest.raises(grpc.RpcError) as error:
            raw_publish(oversized.SerializeToString(), timeout=5)
        assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED

    with BrokerApi(persistent_broker.address) as api:
        assert api.ping().storage_mode == "persistent"
    assert oracle.messages(persistent_broker.db_path) == []
    assert oracle.dead_letters(persistent_broker.db_path) == []
    assert oracle.counts(persistent_broker.db_path) == expected_counts()
    assert oracle.conservation_violations(persistent_broker.db_path) == []
