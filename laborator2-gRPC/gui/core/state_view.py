MESSAGE_COLUMNS = [("MessageId", 250), ("Topic", 90), ("Continut", 180), ("Stare", 90), ("Creat", 160)]
DELIVERY_COLUMNS = [("MessageId", 250), ("Subscriber", 110), ("Stare", 90), ("Incercari", 70), ("Ultima eroare", 180)]
DEAD_LETTER_COLUMNS = [("Id", 40), ("MessageId", 250), ("Subscriber", 100), ("Topic", 80), ("Motiv", 160),
                       ("Incercari", 70), ("Creat", 150), ("Payload", 200)]
SUBSCRIPTION_COLUMNS = [("Subscriber", 160), ("Topic", 160)]


def message_rows(state):
    return [(m.message_id, (m.message_id, m.topic, m.content, m.status, m.created_at_utc)) for m in state.messages]


def delivery_rows(state):
    return [(f"{d.message_id}|{d.subscriber_id}",
             (d.message_id, d.subscriber_id, d.status, str(d.retry_count), d.last_error)) for d in state.deliveries]


def dead_letter_rows(state):
    return [(str(d.id), (str(d.id), d.message_id, d.subscriber_id, d.topic, d.reason, str(d.retry_count),
                         d.created_at_utc, d.raw_payload)) for d in state.dead_letters]


def subscription_rows(state):
    return [(f"{s.subscriber_id}|{s.topic}", (s.subscriber_id, s.topic)) for s in state.subscriptions]


def online_text(state):
    names = ", ".join(state.online_subscribers) if state.online_subscribers else "nimeni"
    return f"Abonati online: {names}"
