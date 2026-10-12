import threading
import time
from concurrent.futures import ThreadPoolExecutor

import oracle
from clients import BrokerApi, ListeningSubscriber, ids, new_id, wait_until
from expectations import expected_counts

PUBLISHERS = 5
PER_PUBLISHER = 40
REDELIVERY_WINDOW = 6.5
TOPICS_BY_SUBSCRIBER = {"sub-a": ["t1"], "sub-b": ["t1", "t2"], "sub-c": ["t2"]}


def test_parallel_publishers_deliver_each_message_exactly_once(broker):
    with BrokerApi(broker.address) as api:
        for subscriber_id, topics in TOPICS_BY_SUBSCRIBER.items():
            for topic in topics:
                api.subscribe(subscriber_id, topic)

    subscribers = {sid: ListeningSubscriber(broker.address, sid) for sid in TOPICS_BY_SUBSCRIBER}
    published = {"t1": [], "t2": []}
    lock = threading.Lock()

    def publish_batch(index):
        with BrokerApi(broker.address) as api:
            for n in range(PER_PUBLISHER):
                topic = "t1" if (index + n) % 2 == 0 else "t2"
                message_id = new_id()
                api.publish(message_id, topic, f"p{index}-m{n}")
                with lock:
                    published[topic].append(message_id)

    try:
        with ThreadPoolExecutor(PUBLISHERS) as pool:
            list(pool.map(publish_batch, range(PUBLISHERS)))

        total = PUBLISHERS * PER_PUBLISHER
        assert len(published["t1"]) + len(published["t2"]) == total

        expected_by_subscriber = {
            sid: {m for topic in topics for m in published[topic]} for sid, topics in TOPICS_BY_SUBSCRIBER.items()}
        for subscriber_id, expected in expected_by_subscriber.items():
            subscribers[subscriber_id].wait_for(lambda ms, e=expected: e <= set(ids(ms)), timeout=60)

        expected_deliveries = 2 * len(published["t1"]) + 2 * len(published["t2"])
        with BrokerApi(broker.address) as api:
            wait_until(
                lambda: sum(1 for d in api.state().deliveries if d.status == "DELIVERED") == expected_deliveries,
                timeout=30, message="all deliveries DELIVERED")
            state = api.state()
            assert len(state.messages) == total
            assert len(state.deliveries) == expected_deliveries

        time.sleep(REDELIVERY_WINDOW)
        for subscriber_id, expected in expected_by_subscriber.items():
            received_ids = ids(subscribers[subscriber_id].messages())
            assert len(received_ids) == len(set(received_ids)) == len(expected), f"{subscriber_id} got duplicates"
            assert set(received_ids) == expected
            assert subscribers[subscriber_id].ack_errors == []
    finally:
        for subscriber in subscribers.values():
            subscriber.close()

    if broker.storage == "persistent":
        assert oracle.counts(broker.db_path) == expected_counts(
            {"DELIVERED": total}, {"DELIVERED": expected_deliveries})
        assert oracle.conservation_violations(broker.db_path) == []
