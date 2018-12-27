import watch


from cadena.abc import WatchABCType, DAGNode


from .proto import GenericNode, CommitData, TreeData, BlobData, LinkMeta
# pls dont remove next line
from .proto import NAMESPACE, DATA


def dump_to_dagnode(packed_node) -> DAGNode:
    return packed_node.dump()


def load_from_dagnode(dag_node: DAGNode):
    if dag_node is None:
        return None

    correspondance = {
        CommitData: Commit,
        TreeData: Tree,
        BlobData: Blob,
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


class LinkDescriptor(watch.WatchMe):
    name = watch.builtins.InstanceOf(str) | watch.builtins.Just(None)
    endpoint = watch.builtins.InstanceOf(bytes)
    link_type = watch.builtins.Just(DATA, NAMESPACE)

    def __init__(self, endpoint, link_type, name=None):
        self.link_type = link_type
        self.name = name
        self.endpoint = endpoint


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

    def compose_generic_node(self):
        return GenericNode(tree=self.packed_payload)

    @property
    def type(self):
        return self.packed_payload.type

    @classmethod
    def from_description(cls, tree_type, link_descriptors: list):
        """
        pairs::list<(string, bytes)>: name-link pairs
        """

        # sanity check block
        if tree_type == DATA:
            all_fine = all(
                descriptor.name is None and descriptor.link_type == DATA
                for descriptor in link_descriptors
            )
        elif tree_type == NAMESPACE:
            all_fine = all(
                descriptor.name is not None for descriptor in link_descriptors
            )
        else:
            all_fine = False

        if not all_fine:
            raise ValueError("Misconfigured Tree node")

        return cls(
            packed_payload=TreeData(
                type=tree_type,
                mdata=[
                    LinkMeta(type=descriptor.link_type, name=descriptor.name)
                    for descriptor in link_descriptors
                ]
            ),
            links=[descriptor.endpoint for descriptor in link_descriptors]
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

