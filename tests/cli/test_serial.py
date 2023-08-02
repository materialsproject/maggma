from maggma.cli.serial import serial
from maggma.core import Builder


class TestBuilder(Builder):
    def __init__(self, total=10):
        self.get_called = 0
        self.process_called = 0
        self.update_called = 0
        super().__init__(sources=[], targets=[])
        self.total = total

    def get_items(self):
        for _i in range(self.total):
            self.get_called += 1
            yield self.get_called

    def process_item(self, item):
        self.process_called += 1
        return item

    def update_targets(self, items):
        self.update_called += 1


def test_serial():
    builder = TestBuilder()

    serial(builder)
    assert builder.get_called == 10
    assert builder.process_called == 10
    assert builder.update_called == 1
