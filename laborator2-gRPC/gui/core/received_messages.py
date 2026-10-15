from dataclasses import dataclass

WAITING = "asteapta ACK"
ACKED = "ACK trimis"
ACKED_DUPLICATE = "ACK duplicat"
RETRANSMISSION = "retransmisie"


@dataclass
class ReceivedMessage:
    message_id: str
    topic: str
    content: str
    attempt: int
    max_attempts: int
    status: str
    retransmission: bool

    def row(self):
        status = f"{self.status}, {RETRANSMISSION}" if self.retransmission else self.status
        return (self.message_id, self.topic, self.content, f"{self.attempt}/{self.max_attempts}", status)


class ReceivedMessages:
    def __init__(self):
        self._items = {}

    def register(self, delivered):
        item = ReceivedMessage(
            delivered.message_id, delivered.topic, delivered.content, delivered.attempt,
            delivered.max_attempts, WAITING, delivered.message_id in self._items)
        self._items[delivered.message_id] = item
        return item

    def clear(self):
        self._items.clear()

    def mark_acked(self, message_id, duplicate):
        item = self._items.get(message_id)
        if item is None:
            return None
        item.status = ACKED_DUPLICATE if duplicate else ACKED
        return item
