import py.test


from cadena.dag import (
    Blob, BlobData, Commit, CommitData, Tree, TreeData, MBlob, MBlobData,
    dump_to_dagnode, load_from_dagnode
)


NODES = [
    (
        Blob(packed_payload=BlobData(data=b"helloworld"), links=[]),
        Blob.from_data(data=b"helloworld"),
    ),
    (
        MBlob(packed_payload=MBlobData(), links=[b"hex0", b"hex1", b"hex2"]),
        MBlob.from_links(links=[b"hex0", b"hex1", b"hex2"]),
    ),
    (
        Tree(
            packed_payload=TreeData(names=["foo.py", "bar.py"]),
            links=[b"hex0", b"hex1"]
        ),
        Tree.from_pairs(
            [("foo.py", b"hex0"), ("bar.py", b"hex1")]
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

