from multiprocessing.connection import Connection
from random import choice
from string import ascii_uppercase
import time
from ifdeploy.deploy import Msg, Program, ProgramId, Topology


def random_string(n: int) -> str:
    return "".join(choice(ascii_uppercase) for _ in range(n))


class SourceProgram(Program):
    n_msg: int

    def program(self, ifconn_out, _msg: Msg):
        time.sleep(2)
        for i in range(SourceProgram.n_msg):
            time.sleep(0.01)
            ifconn_out.send(
                Msg(
                    "source-payload",
                    i,
                    {"id": str(i), "data": random_string(1 << 10)},
                ),
            )
            print("[SOURCE]", "sending")
        self.send_to_parent("finished")


class SinkProgram(Program):
    def __init__(self, name: ProgramId, parent_channel: Connection):
        super().__init__(name, parent_channel)
        self.data = {"counter": 0}
        self.limit = 20

    def program(self, _ifconn_out, msg: Msg):
        if "counter" not in self.data:
            self.data["counter"] = 0
        if msg.tag == "source-payload":
            self.data["counter"] += 1
            print(f'Sink: {self.data["counter"]}')
            if self.data["counter"] >= self.limit:
                self.send_to_parent("finished")


def test(snapshot):
    top = Topology("simple")

    source = top.add_node_group("source", main=SourceProgram)
    sink = top.add_node_group("sink", main=SinkProgram)

    source.balance(sink)

    assert top == snapshot
