import watch


from cadena.abc import IdentifiedDAGNode
from cadena.drivers.common import sha256_id


from .proto import NodeData, CommitData, TreeData, BlobData, MBlobData


def parse_bytes(data: bytes):
    node = NodeData.FromString(data)
    return getattr(node, node.WhichOneof("node_type"))


def protobuf_id_maker(node):
    generic_node = NodeData(**{node.field_name: node.data})
    return sha256_id(data=generic_node.SerializeToString(), links=node.links)


class ProtobufDAGNode(IdentifiedDAGNode):
    def __init__(self, *args, **kwargs):
        super().__init__(id_maker=protobuf_id_maker, *args, **kwargs)


class Commit(ProtobufDAGNode):
    field_name = "commit"
    data = watch.builtins.InstanceOf(CommitData)


class Tree(ProtobufDAGNode):
    field_name = "tree"
    data = watch.builtins.InstanceOf(TreeData)


class Blob(ProtobufDAGNode):
    field_name = "blob"
    data = watch.builtins.InstanceOf(BlobData)


class MBlob(ProtobufDAGNode):
    field_name = "mblob"
    data = watch.builtins.InstanceOf(MBlobData)

