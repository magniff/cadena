import watch


from cadena.abc import WatchABCType, DAGNode


from .proto import GenericNode, CommitData, TreeData, BlobData, MBlobData


def parse_bytes(data: bytes):
    node = GenericNode.FromString(data)
    return getattr(node, node.WhichOneof("node_type"))


def dump_to_dagnode(packed_node):
    return packed_node.dump()


def load_from_dagnode(dag_node):
    correspondance = {
        CommitData: Commit,
        TreeData: Tree,
        BlobData: Blob,
        MBlobData: MBlob,
    }

    generic_node = GenericNode.FromString(dag_node.data)
    node_payload = getattr(generic_node, generic_node.WhichOneof("node_type"))

    return correspondance[type(node_payload)](
        data=node_payload, links=dag_node.links
    )


class PBPackedNode(WatchABCType):

    links = watch.builtins.Container(
        items=watch.builtins.InstanceOf(bytes), container=list
    )

    def dump(self):
        return DAGNode(
            links=self.links,
            data=self.compose_generic_node().SerializeToString(),
        )

    def __init__(self, data, links):
        self.data = data
        self.links = links

    def __eq__(self, other):
        return self.data == other.data and self.links == other.links


class Commit(PBPackedNode):
    data = watch.builtins.InstanceOf(CommitData)

    def compose_generic_node(self):
        return GenericNode(commit=self.data)

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

    def compose_generic_node(self):
        return GenericNode(tree=self.data)

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

    def compose_generic_node(self):
        return GenericNode(blob=self.data)

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

    def compose_generic_node(self):
        return GenericNode(mblob=self.data)

    @classmethod
    def from_links(cls, links: list):
        """
        links::list<bytes>: list of links to other blobs and mblobs
        """
        return cls(links=links, data=MBlobData())

