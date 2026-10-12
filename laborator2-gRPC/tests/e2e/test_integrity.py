import oracle
from clients import BrokerApi, ListeningSubscriber, ids, new_id
from expectations import expected_counts, wait_for_db_status

SAMPLE = 'Ăâîșț ĂÂÎȘȚ „ghilimele” "quotes" \'apostrof\' \\backslash\nlinie nouă\ttab 😀🚀 中文 العربية '


def test_content_is_not_altered(persistent_broker):
    content = (SAMPLE * 200)[:4096]
    assert len(content) == 4096

    with BrokerApi(persistent_broker.address) as api:
        api.subscribe("integrity", "unicode")
        subscriber = ListeningSubscriber(persistent_broker.address, "integrity")
        try:
            message_id = new_id()
            api.publish(message_id, "unicode", content)
            received = subscriber.wait_for(lambda ms: message_id in ids(ms))
            wait_for_db_status(persistent_broker.db_path, message_id, "DELIVERED")
        finally:
            subscriber.close()

    assert received[0].content == content
    assert received[0].content.encode("utf-8") == content.encode("utf-8")
    stored = [row for row in oracle.messages(persistent_broker.db_path) if row[0] == message_id]
    assert stored[0][2] == content
    assert oracle.counts(persistent_broker.db_path) == expected_counts({"DELIVERED": 1}, {"DELIVERED": 1})
    assert oracle.conservation_violations(persistent_broker.db_path) == []
