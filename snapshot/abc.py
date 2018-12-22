from collections.abc import Callable


import abc


from watch import WatchMe
from watch.core import AttributeControllerMeta
from watch.builtins import InstanceOf, SubclassOf, Container


class WatchABCMetaType(abc.ABCMeta, AttributeControllerMeta):
    pass


class WatchABCType(abc.ABC, WatchMe, metaclass=WatchABCMetaType):
    pass


NodeId = bytes
NodePayload = bytes


class AbstractLinkedNode(WatchABCType):

    id = InstanceOf(NodeId)
    data = InstanceOf(NodePayload)
    links = Container(InstanceOf(NodeId), container=list)

    @abc.abstractmethod
    def get_node_id(self, id_getter):
        pass


class AbstractDriver(WatchABCType):

    return_type = SubclassOf(AbstractLinkedNode)
    content_identifier = InstanceOf(Callable)

    @abc.abstractmethod
    def store(self, binary_data: NodePayload) -> NodeId:
        pass

    @abc.abstractmethod
    def retrieve(self, node_id: NodeId) -> AbstractLinkedNode:
        pass


class AbstractHashingDriver(AbstractDriver):
    pass

