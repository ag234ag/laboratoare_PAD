import unittest

from core.broker_options import BrokerOptions


def options(**changes):
    values = {"port": "50051", "storage": "persistent", "db": "broker.db", "workers": "4"}
    values.update(changes)
    return BrokerOptions(**values)


class BrokerOptionsTest(unittest.TestCase):
    def test_valid_options_have_no_errors(self):
        self.assertEqual([], options().errors())

    def test_arguments_follow_the_broker_cli(self):
        self.assertEqual(["--port", "50051", "--storage", "persistent", "--db", "broker.db", "--workers", "4"],
                         options().arguments())

    def test_invalid_port_values(self):
        for port in ("", "0", "65536", "abc", "-1", "5 0", "²"):
            with self.subTest(port=port):
                self.assertEqual(1, len(options(port=port).errors()))

    def test_invalid_workers_storage_and_db(self):
        self.assertEqual(1, len(options(workers="0").errors()))
        self.assertEqual(1, len(options(workers="65").errors()))
        self.assertEqual(1, len(options(storage="disk").errors()))
        self.assertEqual(1, len(options(db="  ").errors()))

    def test_boundaries_are_accepted(self):
        self.assertEqual([], options(port="1", workers="1").errors())
        self.assertEqual([], options(port="65535", workers="64", storage="transient").errors())


if __name__ == "__main__":
    unittest.main()
