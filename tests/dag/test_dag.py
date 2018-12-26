import py.test


from cadena.dag import (
    Blob, BlobData, Commit, CommitData, Tree, TreeData, MBlob, MBlobData
)


NODES = [
    (
        Blob(data=BlobData(data=b"helloworld"), links=[]),
        Blob.from_data(data=b"helloworld"),
    ),
    (
        MBlob(data=MBlobData(), links=[b"hex0", b"hex1", b"hex2"]),
        MBlob.from_links(links=[b"hex0", b"hex1", b"hex2"]),
    ),
    (
        Tree(
            data=TreeData(names=["foo.py", "bar.py"]),
            links=[b"hex0", b"hex1"]
        ),
        Tree.from_pairs(
            [("foo.py", b"hex0"), ("bar.py", b"hex1")]
        )
    ),
    (
        Commit(
            data=CommitData(), links=[b"tree_link", b"parent_link"]
        ),
        Commit.from_tree_parents(tree=b"tree_link", parents=[b"parent_link"]),
    ),
]


@py.test.mark.parametrize("node,constructed_node", NODES)
def test_constructors(node, constructed_node):
    assert node == constructed_node

