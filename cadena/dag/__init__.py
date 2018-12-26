import watch


from cadena.abc import WatchABCType, DAGNode


from .proto import GenericNode, CommitData, TreeData, BlobData, MBlobData


def dump_to_dagnode(packed_node) -> DAGNode:
    return packed_node.dump()


def load_from_dagnode(dag_node: DAGNode):
    if dag_node is None:
        return None

    correspondance = {
        CommitData: Commit,
        TreeData: Tree,
        BlobData: Blob,
        MBlobData: MBlob,
    }

    generic_node = GenericNode.FromString(dag_node.data)
    node_payload = getattr(generic_node, generic_node.WhichOneof("node_type"))

    return correspondance[type(node_payload)](
        packed_payload=node_payload, links=dag_node.links
    )


class _PBPackedNode(WatchABCType):

    links = watch.builtins.Container(
        items=watch.builtins.InstanceOf(bytes), container=list
    )

    def dump(self):
        return DAGNode(
            links=self.links,
            data=self.compose_generic_node().SerializeToString(),
        )

    def __init__(self, packed_payload, links):
        self.packed_payload = packed_payload
        self.links = links

    def __eq__(self, other):
        return (
            self.packed_payload == other.packed_payload and
            self.links == other.links
        )


class Commit(_PBPackedNode):
    packed_payload = watch.builtins.InstanceOf(CommitData)

    def compose_generic_node(self):
        return GenericNode(commit=self.packed_payload)

    @property
    def tree(self) -> bytes:
        return self.links[0]

    @property
    def parents(self) -> list:
        return self.links[1:]

    @classmethod
    def from_tree_parents(cls, tree, parents):
        """
        tree::bytes: link to corresponding tree structure
        parents::list<bytes>: list of links to parent commits
        """
        return cls(
            packed_payload=CommitData(), links=[tree, *parents]
        )


class Tree(_PBPackedNode):
    packed_payload = watch.builtins.InstanceOf(TreeData)

    def subtrees(self):
        pass

    def compose_generic_node(self):
        return GenericNode(tree=self.packed_payload)

    @classmethod
    def from_pairs(cls, pairs: list):
        """
        pairs::list<(string, bytes)>: name-link pairs
        """
        return cls(
            links=[pair[1] for pair in pairs],
            packed_payload=TreeData(names=[pair[0] for pair in pairs])
        )


class Blob(_PBPackedNode):
    packed_payload = watch.builtins.InstanceOf(BlobData)

    @property
    def bytes(self):
        return self.packed_payload.data

    def compose_generic_node(self):
        return GenericNode(blob=self.packed_payload)

    @classmethod
    def from_data(cls, data: bytes):
        """
        data::bytes: binary string associated to the blob
        """
        return cls(
            links=list(), packed_payload=BlobData(data=data),
        )


class MBlob(_PBPackedNode):
    data = watch.builtins.InstanceOf(MBlobData)

    def compose_generic_node(self):
        return GenericNode(mblob=self.packed_payload)

    @classmethod
    def from_links(cls, links: list):
        """
        links::list<bytes>: list of links to other blobs and mblobs
        """
        return cls(links=links, packed_payload=MBlobData())

