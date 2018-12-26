import watch


from cadena.abc import WatchABCType


from .proto import NodeData, CommitData, TreeData, BlobData, MBlobData


def parse_bytes(data: bytes):
    node = NodeData.FromString(data)
    return getattr(node, node.WhichOneof("node_type"))


class PBPackedNode(WatchABCType):

    links = watch.builtins.Container(
        items=watch.builtins.InstanceOf(bytes),
        container=list
    )

    def dump(self):
        raise NotImplementedError()

    def __init__(self, data, links):
        self.data = data
        self.links = links

    def __eq__(self, other):
        return self.data == other.data and self.links == other.links


class Commit(PBPackedNode):
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


class Tree(PBPackedNode):
    data = watch.builtins.InstanceOf(TreeData)

    @classmethod
    def from_pairs(cls, pairs: list):
        """
        pairs::list<(string, bytes)>: name-link pairs
        """
        return cls(
            links=[pair[1] for pair in pairs],
            data=TreeData(names=[pair[0] for pair in pairs])
        )


class Blob(PBPackedNode):
    data = watch.builtins.InstanceOf(BlobData)

    @classmethod
    def from_data(cls, data: bytes):
        """
        data::bytes: binary string associated to the blob
        """
        return cls(
            links=list(), data=BlobData(data=data),
        )


class MBlob(PBPackedNode):
    data = watch.builtins.InstanceOf(MBlobData)

    @classmethod
    def from_links(cls, links: list):
        """
        links::list<bytes>: list of links to other blobs and mblobs
        """
        return cls(links=links, data=MBlobData())

