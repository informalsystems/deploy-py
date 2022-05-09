"""
Topology:
                       ┌───────────────────────────────────────────────────────┐
                       │                                                       │
                       │  ┌───────────────┐                                    │
                       │  │  APALACHE.0   │                                    │
                       │  ├─────────┬─────┤                                    │
                    ┌──┼─►│   IN    │     │              ┌──────────┐          │
 ┌────────┐         │  │  ├─────────┤ OUT ├────────┐     │   SINK   │          │
 │ SOURCE │         │  └─►│ CONTROL │     │        │     ├────┬─────┤          │
 ├──┬─────┤         │     └─────────┴─────┘        └────►│    │     │  FANOUT  │
 │  │     │ BALANCE │                                    │ IN │ OUT ├──────────┤
 │  │ OUT ├─────────┤                              ┌────►│    │     │          │
 │  │     │         │     ┌───────────────┐        │     └────┴─────┘          │
 └──┴─────┘         │     │  APALACHE.1   │        │                           │
                    │     ├─────────┬─────┤        │                           │
                    └────►│   IN    │     │        │                           │
                          ├─────────┤ OUT ├────────┘                           │
                       ┌─►│ CONTROL │     │                                    │
                       │  └─────────┴─────┘                                    │
                       │                                                       │
                       └───────────────────────────────────────────────────────┘

"""


from multiprocessing.connection import Connection
import time

import pytest
from ifdeploy.deploy import Program, ProgramId, Topology, Msg
from random import choice
from string import ascii_uppercase
import logging

N_MSG: int
MSG_SIZE: int


def random_string(n: int) -> str:
    return "".join(choice(ascii_uppercase) for _ in range(n))


class SourceProgram(Program):
    def program(self, ifconn_out, _msg: Msg):
        time.sleep(2)
        for i in range(N_MSG):
            ifconn_out.send(
                Msg(
                    "source-payload",
                    i,
                    {"id": str(i), "data": random_string(MSG_SIZE)},
                ),
            )
        self.send_to_parent("finished")


class ApalacheProgram(Program):
    def program(self, ifconn_out, msg: Msg):
        time.sleep(2)
        ifconn_out.send(
            Msg(
                "apalache-result",
                0,
                {"success": True, "result": "dummy"},
            ),
        )


class SinkProgram(Program):
    def __init__(self, name: ProgramId, parent_channel: Connection):
        super().__init__(name, parent_channel)
        self.data = {}

    def program(self, ifconn_out, msg: Msg):
        if "counter" not in self.data:
            self.data["counter"] = 0
        if msg.tag == "apalache-result":
            self.data["counter"] += 1
            logging.info("[Sink]", f"{self.data['counter']}")
            if self.data["counter"] >= N_MSG:
                self.send_to_parent("finished")
                ifconn_out.send(
                    Msg("terminate", 0, {}),
                )


@pytest.mark.parametrize("n_msg", [100, 500, 1000])
@pytest.mark.parametrize("msg_size", [1000])
def test_with_params(snapshot, n_msg, msg_size):
    global N_MSG, MSG_SIZE
    N_MSG = int(n_msg)
    MSG_SIZE = int(msg_size)
    top = Topology("parallel-controller")

    source = top.add_node_group("source", main=SourceProgram)
    apalache = top.add_node_group("apalache", main=ApalacheProgram, scale=10)
    sink = top.add_node_group("sink", main=SinkProgram)

    source.balance(apalache, msg_tags=["source-payload"])
    apalache.balance(sink, msg_tags=["apalache-result"])
    sink.fanout(apalache, msg_tags=["terminate"])

    assert top == snapshot
