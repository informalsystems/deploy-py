"""
Topology:
 ┌────────┐              ┌──────┐
 │ SOURCE │              │ SINK │
 ├──┬─────┤              ├────┬─┤
 │  │     │   BALANCE    │    │ │
 │  │ OUT ├─────────────►│ IN │ │
 │  │     │              │    │ │
 └──┴─────┘              └────┴─┘
"""


from multiprocessing.connection import Connection
import time
from typing import List

import pytest
from ifdeploy.deploy import Msg, Program, ProgramId, Topology, NodeGroup
from random import choice
from string import ascii_uppercase
import logging


def random_string(n: int) -> str:
    return "".join(choice(ascii_uppercase) for _ in range(n))


class SourceProgram(Program):
    n_msg: int

    def program(self, ifconn_out, _msg: Msg):
        time.sleep(2)
        for i in range(SourceProgram.n_msg):
            # time.sleep(0.01)
            ifconn_out.send(
                Msg(
                    "source-payload",
                    i,
                    {"id": str(i), "data": random_string(1 << 10)},
                ),
            )
            logging.info("[SOURCE]", "sending")
        self.send_to_parent("finished")


class SinkProgram(Program):
    limit: int

    def __init__(self, name: ProgramId, parent_channel: Connection):
        super().__init__(name, parent_channel)
        self.data = {}

    def program(self, _ifconn, msg: Msg):
        if "counter" not in self.data:
            self.data["counter"] = 0
        if msg.tag == "source-payload":
            self.data["counter"] += 1
            logging.info("[Sink]", f"{self.data['counter']}")
            if self.data["counter"] >= SinkProgram.limit:
                self.send_to_parent("finished")


@pytest.mark.parametrize("n_msg", [100, 500, 1000])
@pytest.mark.parametrize("n_queue", [10, 20, 30])
def test_with_params(snapshot, n_msg, n_queue):
    top = Topology("queue-fanout")
    n_msg = int(n_msg)
    n_queue = int(n_queue)

    SourceProgram.n_msg = n_msg
    SinkProgram.limit = n_msg * n_queue

    source = top.add_node_group("source", main=SourceProgram)
    queues: List[NodeGroup] = [top.add_queue(f"queue-{i}") for i in range(n_queue)]
    sink = top.add_node_group("sink", main=SinkProgram)

    source.fanout(
        queues,
        msg_tags=["source-payload"],
    )

    for queue in queues:
        queue.balance(sink, msg_tags=["source-payload"])

    assert top == snapshot
