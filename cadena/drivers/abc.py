from abc import abstractmethod
from collections.abc import Callable


from watch.builtins import InstanceOf, Container


from cadena.abc import WatchABCType


class DAGNode(WatchABCType):
    data = InstanceOf(bytes)
    links = Container(InstanceOf(bytes), container=list)

    def __eq__(self, other):
        return (
            isinstance(self, type(other)) and
            self.data == other.data and
            self.links == other.links
        )

    def __init__(self, data, links):
        self.data = data
        self.links = links


class AbstractDriver(WatchABCType):
    """
    store  :: dag_node -> bytes
    lookup :: bytes -> dag_node
    """

    node_identifier = InstanceOf(Callable)

    def __init__(self, node_identifier):
        self.node_identifier = node_identifier

    @abstractmethod
    def store(self, node: DAGNode) -> bytes:
        pass

    @abstractmethod
    def lookup(self, node_id: bytes) -> DAGNode:
        pass

