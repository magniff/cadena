import watch
from watch.builtins import InstanceOf, Predicate, Container


from cadena.abc import WatchABCType, DAGNode


from .proto import GenericNode, CommitData, TreeData, BlobData, LinkMeta
# pls dont remove next line
from .proto import NAMESPACE, DATA


SpanChecker = (
    Container(items=InstanceOf(int), container=tuple) &
    Predicate(lambda container: len(container) == 2) &
    Predicate(lambda container: container[1] >= container[0])
)


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
    endpoint = watch.builtins.InstanceOf(bytes)

    def dump_to_pb_link_meta(self):
        pass

    def __init__(self, endpoint):
        self.endpoint = endpoint


class NSLink(LinkDescriptor):
    name = watch.builtins.InstanceOf(str)

    def dump_to_pb_link_meta(self):
        return LinkMeta(type=NAMESPACE, name=self.name)

    def __init__(self, endpoint, name):
        self.name = name
        self.endpoint = endpoint


class DataLink(LinkDescriptor):
    span = SpanChecker()

    def dump_to_pb_link_meta(self):
        return LinkMeta(
            type=DATA, span_from=self.span[0], span_to=self.span[1]
        )

    def __init__(self, endpoint, span):
        self.span = span
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

        # sanity check block
        if tree_type == DATA:
            if not all(isinstance(link, DataLink) for link in link_descriptors):
                raise ValueError()

        return cls(
            packed_payload=TreeData(
                type=tree_type,
                mdata=[
                    descriptor.dump_to_pb_link_meta() for descriptor in
                    link_descriptors
                ]
            ),
            links=[
                descriptor.endpoint for descriptor in link_descriptors
            ]
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
            packed_payload=BlobData(data=data),
            links=list(),
        )

