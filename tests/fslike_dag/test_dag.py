import py.test


from cadena.fslike_dag import (
    Blob, BlobData, Commit, CommitData, Tree, TreeData, LinkDescriptor,
    dump_to_dagnode, load_from_dagnode, DATA, NAMESPACE, LinkMeta
)


NODES = [
    (
        Blob(packed_payload=BlobData(data=b"helloworld"), links=[]),
        Blob.from_data(data=b"helloworld"),
    ),
    (
        Tree(
            packed_payload=TreeData(
                type=DATA,
                mdata=[
                    LinkMeta(type=DATA, name=None),
                    LinkMeta(type=DATA, name=None),
                ]
            ),
            links=[b"hex0", b"hex1"]
        ),
        Tree.from_description(
            tree_type=DATA,
            link_descriptors=[
                LinkDescriptor(endpoint=b"hex0", link_type=DATA),
                LinkDescriptor(endpoint=b"hex1", link_type=DATA),
            ]
        )
    ),
    (
        Tree(
            packed_payload=TreeData(
                type=NAMESPACE,
                mdata=[
                    LinkMeta(type=DATA, name="helloworld"),
                    LinkMeta(type=NAMESPACE, name="morestuff"),
                ]
            ),
            links=[b"hex0", b"hex1"]
        ),
        Tree.from_description(
            tree_type=NAMESPACE,
            link_descriptors=[
                LinkDescriptor(
                    name="helloworld", endpoint=b"hex0", link_type=DATA
                ),
                LinkDescriptor(
                    name="morestuff", endpoint=b"hex1", link_type=NAMESPACE
                ),
            ]
        )
    ),
    (
        Commit(
            packed_payload=CommitData(), links=[b"tree_link", b"parent_link"]
        ),
        Commit.from_tree_parents(tree=b"tree_link", parents=[b"parent_link"]),
    ),
]


@py.test.mark.parametrize("node,constructed_node", NODES)
def test_constructors(node, constructed_node):
    assert node == constructed_node


@py.test.mark.parametrize("node,constructed_node", NODES)
def test_dump_load(node, constructed_node):
    node_as_simple_dag_node = dump_to_dagnode(node)
    assert node_as_simple_dag_node == dump_to_dagnode(constructed_node)
    assert load_from_dagnode(node_as_simple_dag_node) == node

