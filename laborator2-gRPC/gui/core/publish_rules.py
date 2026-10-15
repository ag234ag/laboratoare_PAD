import grpc

MAX_NAME_LENGTH = 64
MAX_CONTENT_LENGTH = 4096


def _validate_name(value, label):
    if not value.strip():
        return f"{label} nu poate fi gol"
    if len(value) > MAX_NAME_LENGTH:
        return f"{label} are peste {MAX_NAME_LENGTH} de caractere"
    if not all(ch.isalnum() or ch in "._-" for ch in value):
        return f"{label} poate contine doar litere, cifre, '.', '_' si '-'"
    return None


def validate_topic(topic):
    return _validate_name(topic, "topicul")


def validate_subscriber_id(subscriber_id):
    return _validate_name(subscriber_id, "Subscriber ID")


def validate_content(content):
    if not content.strip():
        return "mesajul nu poate fi gol"
    if len(content) > MAX_CONTENT_LENGTH:
        return f"mesajul are peste {MAX_CONTENT_LENGTH} de caractere"
    return None


def publish_label(reply=None, error=None):
    if error is None:
        return f"OK, {reply.subscribers} abonati"
    if not isinstance(error, grpc.RpcError):
        return str(error)
    code = error.code()
    if code == grpc.StatusCode.ALREADY_EXISTS:
        return "DUPLICATE"
    if code == grpc.StatusCode.INVALID_ARGUMENT:
        return "INVALID -> DLQ"
    if code == grpc.StatusCode.UNAVAILABLE:
        return "INDISPONIBIL"
    return f"{code.name} - {error.details()}"
