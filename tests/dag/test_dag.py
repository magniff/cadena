import py.test


from cadena.dag import (
    Blob, BlobData, Commit, CommitData, Tree, TreeData, MBlob, MBlobData
)


NODES = [
    Blob(data=BlobData(data=b"helloworld"), links=[]),
    MBlob(data=MBlobData(), links=[b"hex0", b"hex1", b"hex2"]),
    Tree(data=TreeData(names=["foo.py", "bar.py"]), links=[b"hex0", b"hex1"]),
    Commit(
        data=CommitData(),
        links=[b"hex0", b"hex1"]
    ),
]


@py.test.mark.parametrize("node", NODES)
def test_hello(node):
    assert node.id


