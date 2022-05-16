"""Provides python classes for a Informal.Deploy topology.
1. Build Nodes and Queues.
2. Connect them and construct a Topology
3. Deploy the connected Nodes and Queues and wait until they finish.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
import json
from multiprocessing import Pipe  # , Process
from threading import Thread as Process
from multiprocessing.connection import Connection
from threading import Thread
import time
from typing import Dict, Final, List, Optional, Type, TypeVar, Tuple
from typing_extensions import Self
import logging

from iconnect.network import Network, SendPort, RecvPort

TopologyId = str
NodeGroupId = str
NodeId = str
ProgramId = str
PortId = str
MsgTag = str


class Distribution(Enum):
    balance = "balance"
    fanout = "fanout"


@dataclass
class Msg:
    """Msg Dataclass"""

    tag: MsgTag
    id: int
    data: Dict


class Program(ABC):
    """Program class for Nodes."""

    OutConnection = Tuple[Distribution, List[Self], List[MsgTag] | None]
    InConnection = Tuple[Distribution, Self, List[MsgTag] | None]

    _in_port: Final[str] = "in"
    _out_port: Final[str] = "out"

    _iconn_recv_port: RecvPort
    _iconn_send_port: SendPort

    def __init__(self, network: Network, name: ProgramId, parent_channel: Connection):
        """Initialize the Program.

        Args:
            name (ProgramId): Name or Id of the program.
            parent_channel (Node): Channel to parent of this Program.
        """
        self.network = network
        self.name: ProgramId = name
        self.parent_channel: Connection = parent_channel
        self.out_connections: List[Program.OutConnection] = []
        self.in_connections: List[Program.InConnection] = []

        self._iconn_recv_port = self.network.add_recv_port(self._port_id(self._in_port))
        self._iconn_send_port = self.network.add_send_port(
            self._port_id(self._out_port)
        )

        self._is_terminated: bool = False

    @abstractmethod
    def program(self, ifconn_out, msg: Msg):
        """Handle program execution.

        Args:
            msg (Msg): IfDeploy Msg object for the handler.
        """

    def _port_id(self, name: PortId) -> str:
        """Returns a unique port id for this program.

        Args:
            name (PortId): A port id for this program.

        Returns:
            str: Unique port id.
        """
        return f"{self.name}.{name}"

    def add_connection(
        self: Self,
        distribution: Distribution,
        others: List[Self],
        *,
        msg_tags: List[MsgTag] | None = None,
    ):
        """Connect this program to other program using a specific distribution strategy.

        Args:
            distribution (Distribution): Distribution type -- balance or fanout.
            others (List[]): Other programs to connect.
            msg_tags (List[MsgTag], optional): Enables message tag filtering. Defaults to any tags.
        """
        match distribution:
            case Distribution.balance:
                self._iconn_send_port.balance([o._iconn_recv_port for o in others])
            case Distribution.fanout:
                self._iconn_send_port.fanout([o._iconn_recv_port for o in others])
        self.out_connections.append((distribution, others, msg_tags))
        for other in others:
            other.in_connections.append((distribution, self, msg_tags))

    def is_source(self) -> bool:
        return len(self.in_connections) == 0

    def run(self, *, poll_interval: float):
        """Run the program in a loop, unless the node is a root node.

        This one is called inside Informal.connect.init_thread

        Args:
            poll_interval (float): Poll interval for the running program.
        """
        self.network.sync([self._iconn_recv_port, self._iconn_send_port])

        if self.is_source():
            self.program(self._iconn_send_port, Msg("source", 0, {}))
        else:
            thread = None
            while True:
                self.recv_from_parent()
                if self.is_terminated():
                    break
                msg = self._iconn_recv_port.recv()
                if msg:
                    thread = Thread(
                        target=self.program, args=(self._iconn_send_port, msg)
                    )
                    thread.start()
                    while True:
                        thread.join(poll_interval)
                        if not thread.is_alive():
                            break
                        self.recv_from_parent()
                        if self.is_terminated():
                            break
            # no way to stop a thread
            if thread is not None:
                thread.join()
        self._iconn_send_port.close()

    def recv_from_parent(self):
        """Poll for control command from parent_channel"""
        if self.parent_channel.poll():
            control_msg = self.parent_channel.recv()
            match control_msg:
                case "terminate":
                    self._is_terminated = True

    def send_to_parent(self, msg):
        """Send msg to parent_channel"""
        self.parent_channel.send(msg)

    def is_terminated(self) -> bool:
        """Check if the program is terminated.

        Returns:
            bool: if the program is terminated.
        """
        return self._is_terminated

    def __repr__(self):
        return json.dumps(self, cls=DeployEncoder, indent=2)


class QueueProgram(Program):
    """Default implementation of QueueProgram."""

    def program(self, ifconn_out, msg: Msg):
        logging.info(
            f"[QUEU] {self.name} - relaying {msg.tag}",
        )
        # pass any incoming messages to its default out port.
        ifconn_out.send(self._port_id(self._out_port), msg)


class ControlProgram(Program):
    """Default implementation of ControlProgram."""

    def program(self, ifconn_out, msg: Msg):
        # TODO add more controls
        # right now, if control program terminates if it receives
        # a message with "terminate" tag.
        if msg.tag == "terminate":
            self.parent_channel.send("terminate")


# TypeVar restricted to derived Program class.
P = TypeVar("P", bound=Program)


class Node:
    """Node class."""

    def __init__(
        self,
        network: Network,
        name: NodeId,
        seq: Tuple[int, int],
        *,
        main: Type[P] | None = None,
        control: Type[P] | None = None,
    ):
        """Initialize the instance of this class .

        Args:
            network (Network): A network
            name (NodeId): Name for this Node
            seq (int): Sequence id for this node for its parent node group
            program (Type[P], optional): A Program derived class type - the main program of the node. Defaults to `QueueProgram`.
            control (Type[P], optional): A Program derived class type - the control program of the node. Defaults to `ControlProgram`.
        """
        self.network = network
        self.name = name
        self.seq: int = seq[0]
        self.scale: int = seq[1]
        self.main: P
        self.control: Optional[P]
        self.children_channel = []
        parent_channel_main, child_channel_main = Pipe()
        if main:
            parent_channel_control, child_channel_control = Pipe()
            self.main = main(
                self.network, f"{name}.{self.seq}.main", child_channel_main
            )
            if control:
                self.control = control(
                    self.network, f"{name}.{self.seq}.control", child_channel_control
                )
            else:
                self.control = ControlProgram(
                    self.network, f"{name}.{self.seq}.control", child_channel_control
                )
            self.children_channel.append(parent_channel_main)
            self.children_channel.append(parent_channel_control)
        else:
            self.main = QueueProgram(self.network, f"{name}.queue", child_channel_main)
            self.control = None

    def add_connectionn(
        self: "Node",
        distribution: Distribution,
        other: List[Self | Program],
        *,
        msg_tags: List[MsgTag] | None = None,
    ):
        """Connect a other programs to this Node.

        Args:
            distribution (Distribution): Distribution strategy.
            others (List[Program]): Other programs.
            msg_tags (List[MsgTag], optional): Enables message tag filtering. Defaults to any tag.
        """
        other_ = []
        if msg_tags is None:
            msg_tags = ["*"]
        for e in other:
            match e:
                case Node():
                    other_.append(e.main)
                case Program():
                    other_.append(e)
        self.main.add_connection(distribution, other_, msg_tags=msg_tags)

    def run(self, *, poll_interval: float):
        """Run the node with its Main and Control programs concurrently.

        Args:
            poll_interval (float): Poll interval for the running processes.
        """
        processes = []
        if self.control:
            control_handle = Process(
                target=self.control.run, kwargs={"poll_interval": poll_interval}
            )
            processes.append(control_handle)
            program_handle = Process(
                target=self.main.run, kwargs={"poll_interval": poll_interval}
            )
            processes.append(program_handle)

        control_recv_handle = Process(
            target=self.recv_control, kwargs={"poll_interval": poll_interval}
        )
        processes.append(control_recv_handle)

        for process in processes:
            process.start()
        for (i, process) in enumerate(processes):
            res = process.join()
            logging.info(f"[NOD] Node {self.name}.{self.seq} has joined: {i} {res}")

    def recv_control(self, *, poll_interval: float = 0.1):
        """Receive control command from Control program

        Args:
            poll_interval (float, optional): Poll interval to receeive message. Defaults to 0.1.
        """
        while True:
            time.sleep(poll_interval)
            for child_channel in self.children_channel:
                if child_channel.poll():
                    msg = child_channel.recv()
                    if msg == "terminate":
                        self.terminate()

    def terminate(self):
        """Terminate the Node.

        This should stop all the running loops.
        """
        for child_channel in self.children_channel:
            child_channel.send("terminate")

    def __repr__(self):
        return json.dumps(self, cls=DeployEncoder, indent=2)


class NodeGroup:
    """A NodeGroup class for a group of similary instantiated Node."""

    name: NodeGroupId
    count: int
    nodes: List[Node]

    def __init__(
        self,
        network: Network,
        name: NodeGroupId,
        *,
        main: Type[P] | None = None,
        control: Type[P] | None = None,
        scale: int = 1,
    ):
        """Initialize the node.

        Args:
            network (Network): A network
            name (NodeGroupId): Name for this node group.
            program (Type[P], optional): Main program for the Nodes in this NodeGroup. Defaults to QueueProgram.
            control (Type[P], optional): Control program for the nodes in this NodeGroup. Defaults to ControlProgram.
            count (int, optional): Number of nodes to instantiate in this NodeGroup. Defaults to 1.
        """
        self.network = network
        self.name = name
        self.count = scale
        self.nodes = [
            Node(self.network, self.name, (seq, scale), main=main, control=control)
            for seq in range(scale)
        ]

    @property
    def programs(self) -> List[Program]:
        """Returns the Main Program objects of the Nodes associated of to this NodeGroup.

        Returns:
            List[Program]: List of Main programs.
        """
        return [node.main for node in self.nodes]

    @property
    def controls(self) -> List[Program]:
        """Returns the Control Program objects of the Nodes associated of to this NodeGroup.

        Returns:
            List[Program]: List of Control programs.
        """
        return [node.control for node in self.nodes if node.control]

    def add_connection(
        self,
        distribution: Distribution,
        others: Self | List[Self] | List[Program] | List[List[Program]],
        *,
        msg_tags: List[MsgTag] | None = None,
    ):
        """Connect a other programs to this NodeGroup.

        Args:
            distribution (Distribution): Distribution strategy.
            programs_from_node_groups (List[List[Program]]): Other programs.
            msg_tags (List[MsgTag], optional): Enables message tag filtering. Defaults to any tag.
        """
        if others:
            if isinstance(others, NodeGroup):
                others = [others]
            match others[0]:
                case NodeGroup():
                    others_ = []
                    for other in others:
                        others_.extend([node.main for node in other.nodes])
                case Program():
                    others_ = others
                case list():
                    others_ = []
                    for other in others:
                        others_.extend(other)

            for node in self.nodes:
                node.add_connectionn(distribution, others_, msg_tags=msg_tags)

    def main(self) -> List[Program]:
        return [node.main for node in self.nodes]

    def control(self) -> List[Program]:
        return [node.control for node in self.nodes if node.control]

    def balance(
        self,
        others: Self | List[Self] | List[Program] | List[List[Program]],
        *,
        msg_tags: List[MsgTag] | None = None,
    ):
        """Convenient method for `add_connection` with Balance strategy

        Args:
            programs_from_node_groups (List[List[Program]]): Other programs.
            msg_tags (List[MsgTag], optional): Enables message tag filtering. Defaults to any tag.
        """
        self.add_connection(
            Distribution.balance,
            others,
            msg_tags=msg_tags,
        )

    def fanout(
        self,
        others: Self | List[Self] | List[Program] | List[List[Program]],
        *,
        msg_tags: List[MsgTag] | None = None,
    ):
        """Convenient method for `add_connection` with Fanout strategy

        Args:
            programs_from_node_groups (List[List[Program]]): Other programs.
            msg_tags (List[MsgTag], optional): Enables message tag filtering. Defaults to any tag.
        """
        self.add_connection(
            Distribution.fanout,
            others,
            msg_tags=msg_tags,
        )

    def run(self, *, poll_interval: float):
        """Run all the Nodes in this NodeGroup concurrently.

        Args:
            poll_interval (float): Poll interval of the running Nodes
        """
        processes = [
            Process(target=node.run, kwargs={"poll_interval": poll_interval})
            for node in self.nodes
        ]
        for process in processes:
            process.start()
        for (i, process) in enumerate(processes):
            res = process.join()
            logging.info(f"[NODG] Node {self.name}.{i} has joined: {res}")

    def terminate(self):
        """Terminate all the nodes in this NodeGroup"""
        for node in self.nodes:
            node.terminate()

    def __repr__(self):
        return json.dumps(self, cls=DeployEncoder, indent=2)


class Topology:
    """A top level class to construct the underlying topology and deploy."""

    def __init__(self, network: Network, name: TopologyId):
        """Initialize the Topology class.

        Args:
            network (Network): A network
            name (TopologyId): Name for this Topolgy
        """
        self.network = network
        self.name: TopologyId = name
        self.node_groups: Dict[NodeGroupId, NodeGroup] = {}
        self.queues: Dict[NodeId, NodeGroup] = {}

    def add_node_group(
        self,
        name: NodeGroupId,
        *,
        main: Type[P],
        control: Type[P] | None = None,
        scale: int = 1,
    ) -> NodeGroup:
        """Add a node group to the Topology.

        Args:
            name (NodeGroupId): Name for this node group.
            main (Type[P]): Main program for the Nodes in this NodeGroup.
            control (Type[P], optional): Control program for the nodes in this NodeGroup. Defaults to ControlProgram.
            scale (int, optional): [description]. Defaults to 1.

        Returns:
            NodeGroup: Created NodeGroup
        """
        node_group = NodeGroup(
            self.network,
            name,
            main=main,
            control=control,
            scale=scale,
        )
        self.node_groups[name] = node_group
        return node_group

    def add_queue(self, name: NodeId) -> NodeGroup:
        """Create a new queue to the Topology.

        Returns:
            NodeGroup: Created Queue
        """
        queue = NodeGroup(self.network, name)
        self.queues[name] = queue
        return queue

    def deploy(self, *, poll_interval: float = 0.2):
        """Deploy the topology and run all the nodes and the queues concurrently.

        Args:
            poll_interval (float, optional): Poll interval for the deployed processes. Defaults to 0.2.
        """
        queue_handles = {
            Process(
                target=queue.run, kwargs={"poll_interval": poll_interval}
            ): queue_name
            for (queue_name, queue) in self.queues.items()
        }
        node_handles = {
            Process(
                target=node_group.run, kwargs={"poll_interval": poll_interval}
            ): node_group_name
            for (node_group_name, node_group) in self.node_groups.items()
        }

        for p in queue_handles:
            p.start()

        for p in node_handles:
            p.start()

        for p in node_handles:
            logging.info(
                f"[SYST] Node group '{node_handles[p]}' is completed : {p.join()}"
            )

        for node in self.node_groups.values():
            node.terminate()

        for queue in self.queues.values():
            queue.terminate()

        for p in queue_handles:
            logging.info(f"[SYST] Queue '{queue_handles[p]}' is completed : {p.join()}")

    def __repr__(self):
        return json.dumps(self, cls=DeployEncoder, indent=2)


class DeployEncoder(json.JSONEncoder):
    """JSON encoder for deploy classes"""

    def default(self, o):
        match o:
            case Topology():
                queue_dict = {
                    name: queue.nodes[0] for (name, queue) in o.queues.items()
                }
                node_dict = {name: ng.nodes[0] for (name, ng) in o.node_groups.items()}
                data = {}
                if queue_dict:
                    data["queues"] = queue_dict
                if node_dict:
                    data["nodes"] = node_dict
                return {o.name: data}
            case Node():
                program_dict = {}
                if o.scale > 1:
                    program_dict["scale"] = o.scale
                if o.main.out_connections:
                    program_dict["main"] = o.main.out_connections
                if o.control and o.control.out_connections:
                    program_dict["control"] = o.control.out_connections
                return program_dict
            case NodeGroup():
                data = super().default(o.nodes[0])
                data["scale"] = len(o.nodes)
                return data
            case Program():
                return o.name
            case Distribution():
                match o:
                    case Distribution.balance:
                        return "balance"
                    case Distribution.fanout:
                        return "fanout"
            case set():
                return list(o)
            case _:
                return super().default(o)
