import types
import unittest

import grpc

from core import publish_rules
from tests.support import FakeRpcError


class ValidationTest(unittest.TestCase):
    def test_valid_topics_include_unicode_letters_like_the_broker(self):
        for topic in ("sport.live_1-a", "ăîșț", "x" * 64):
            with self.subTest(topic=topic):
                self.assertIsNone(publish_rules.validate_topic(topic))

    def test_invalid_topics(self):
        for topic in ("", "   ", "a b", "a/b", "a\nb", "x" * 65):
            with self.subTest(topic=topic):
                self.assertIsNotNone(publish_rules.validate_topic(topic))

    def test_subscriber_id_uses_the_same_rule(self):
        self.assertIsNone(publish_rules.validate_subscriber_id("gui-a1"))
        self.assertIn("Subscriber ID", publish_rules.validate_subscriber_id(""))
        self.assertIsNotNone(publish_rules.validate_subscriber_id("a b"))

    def test_content_rules(self):
        self.assertIsNone(publish_rules.validate_content("salut ă\nx"))
        self.assertIsNotNone(publish_rules.validate_content("   "))
        self.assertIsNotNone(publish_rules.validate_content("x" * 4097))


class PublishLabelTest(unittest.TestCase):
    def test_ok_shows_subscriber_count(self):
        self.assertEqual("OK, 2 abonati", publish_rules.publish_label(reply=types.SimpleNamespace(subscribers=2)))
        self.assertEqual("OK, 0 abonati", publish_rules.publish_label(reply=types.SimpleNamespace(subscribers=0)))

    def test_known_error_codes(self):
        label = publish_rules.publish_label
        self.assertEqual("DUPLICATE", label(error=FakeRpcError(grpc.StatusCode.ALREADY_EXISTS)))
        self.assertEqual("INVALID -> DLQ", label(error=FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT)))
        self.assertEqual("INDISPONIBIL", label(error=FakeRpcError(grpc.StatusCode.UNAVAILABLE)))

    def test_other_errors(self):
        self.assertEqual("INTERNAL - x", publish_rules.publish_label(error=FakeRpcError(grpc.StatusCode.INTERNAL, "x")))
        self.assertEqual("boom", publish_rules.publish_label(error=RuntimeError("boom")))


if __name__ == "__main__":
    unittest.main()
