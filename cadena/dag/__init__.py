import watch


from cadena.abc import IdentifiedDAGNode
from cadena.drivers.common import sha256_id


from .proto import NodeData, CommitData, TreeData, BlobData, MBlobData


def parse_bytes(data: bytes):
    node = NodeData.FromString(data)
    return getattr(node, node.WhichOneof("node_type"))


def protobuf_id_maker(node):
    return sha256_id(**node.to_mapping())


class ProtobufDAGNode(IdentifiedDAGNode):
    data = watch.builtins.InstanceOf(NodeData)

    def __eq__(self, other):
        return self.data == other.data and self.links == other.links

    def to_mapping(self):
        return {
            "links": self.links,
            "data": NodeData(
                **{
                    type(self).__qualname__.lower(): self.data
                }
            ).SerializeToString()
        }

    def __init__(self, *args, **kwargs):
        super().__init__(id_maker=protobuf_id_maker, *args, **kwargs)


class Commit(ProtobufDAGNode):
    data = watch.builtins.InstanceOf(CommitData)

    @classmethod
    def from_tree_parents(cls, tree, parents):
        """
        tree::bytes: link to corresponding tree structure
        parents::list<bytes>: list of links to parent commits
        """
        return cls(
            data=CommitData(), links=[tree, *parents]
        )


class Tree(ProtobufDAGNode):
    data = watch.builtins.InstanceOf(TreeData)

    @classmethod
    def from_names_links(cls, names, links):
        """
        names::list<str>: list of "filenames"
        links::list<bytes>: list of links to "file" structures
        """
        return cls(
            links=links, data=TreeData(names=names)
        )


class Blob(ProtobufDAGNode):
    data = watch.builtins.InstanceOf(BlobData)

    @classmethod
    def from_data(cls, data: bytes):
        """
        data::bytes: binary string associated to the blob
        """
        return cls(
            links=list(), data=BlobData(data=data),
        )


class MBlob(ProtobufDAGNode):
    data = watch.builtins.InstanceOf(MBlobData)

    @classmethod
    def from_links(cls, links: list):
        """
        links::list<bytes>: list of links to other blobs and mblobs
        """
        return cls(links=links, data=MBlobData())

