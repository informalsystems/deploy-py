"""
Topology:

  ┌────────┐   ┌────────┐   ┌────────┐   ┌────────┐   ┌────────┐   ┌────────┐
  │ source ├──►│ input  ├──►│  tlc   ├──►│ filter ├──►│ output ├──►│  sink  │
  └────────┘   └────────┘   └────────┘   └────────┘   └────────┘   └────────┘

"""

N_MSG: int
MSG_SIZE: int
N_OUT_MSG: int
OUT_MSG_SIZE: int

from multiprocessing.connection import Connection
import time

import pytest
from ifdeploy.deploy import Program, ProgramId, Topology, Msg
from random import choice
from string import ascii_uppercase
import logging


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


class TLCProgram(Program):
    def program(self, ifconn_out, msg: Msg):
        for i in range(N_OUT_MSG):
            ifconn_out.send(
                Msg(
                    "tlc-result",
                    i,
                    {"id": str(i), "result": random_string(OUT_MSG_SIZE)},
                ),
            )


class FilterProgram(Program):
    def program(self, ifconn_out, msg: Msg):
        if msg.id % 100 == 9:
            ifconn_out.send(msg)


class SinkProgram(Program):
    def __init__(self, name: ProgramId, parent_channel: Connection):
        super().__init__(name, parent_channel)
        self.data = {}

    def program(self, ifconn_out, msg: Msg):
        if "counter" not in self.data:
            self.data["counter"] = 0
        if msg.tag == "tlc-result":
            self.data["counter"] += 1
            logging.info("[Sink]", f"{self.data['counter']}")
            if self.data["counter"] >= int(N_MSG * N_OUT_MSG / 100):
                self.send_to_parent("finished")
                ifconn_out.send(
                    Msg("terminate", 0, {}),
                )


@pytest.mark.parametrize("n_msg", [100, 500, 1000])
@pytest.mark.parametrize("msg_size", [1000])
@pytest.mark.parametrize("n_out_msg", [100, 500, 1000])
@pytest.mark.parametrize("out_msg_size", [10000])
@pytest.mark.parametrize("count", [1, 2, 3])
def test_with_params(snapshot, n_msg, msg_size, n_out_msg, out_msg_size, count):
    global N_MSG, MSG_SIZE, N_OUT_MSG, OUT_MSG_SIZE
    N_MSG = int(n_msg)
    MSG_SIZE = int(msg_size)
    N_OUT_MSG = int(n_out_msg)
    OUT_MSG_SIZE = int(out_msg_size)
    COUNT = int(count)

    top = Topology("test-filter")

    source = top.add_node_group("source", main=SourceProgram)
    input_q = top.add_queue("input")
    tlc = top.add_node_group("tlc", main=TLCProgram, scale=COUNT)
    filter_n = top.add_node_group("filter", main=FilterProgram, scale=COUNT)
    output = top.add_queue("output")
    sink = top.add_node_group("sink", main=SinkProgram)

    source.balance(input_q, msg_tags=["source-payload"])
    input_q.balance(tlc, msg_tags=["source-payload"])
    tlc.balance(filter_n, msg_tags=["tlc-result"])
    filter_n.balance(output, msg_tags=["tlc-result"])
    output.balance(sink, msg_tags=["tlc-result"])
    sink.fanout([tlc.control(), filter_n.control()], msg_tags=["terminate"])

    assert top == snapshot
