import py.test


from cadena.fslike_dag import (
    Blob, BlobData, Commit, CommitData, Tree, TreeData, NamedLink, UnnamedLink,
    dump_to_dagnode, load_from_dagnode, DATA, NAMESPACE, LinkMeta,
    ChunkEndpoint, NamespaceEndpoint
)


NODES = [
    # simple blob node
    (
        Blob(packed_payload=BlobData(data=b"helloworld"), links=[]),
        Blob.from_data(data=b"helloworld"),
    ),
    # data Tree
    (
        Tree(
            packed_payload=TreeData(
                type=DATA,
                mdata=[
                    LinkMeta(type=DATA, name=None, span_from=0, span_to=10),
                    LinkMeta(type=DATA, name=None, span_from=10, span_to=20),
                ]
            ),
            links=[b"hex0", b"hex1"]
        ),
        Tree.from_links_description(
            link_descriptors=[
                UnnamedLink(
                    endpoint=ChunkEndpoint(id=b"hex0", span=(0, 10))
                ),
                UnnamedLink(
                    endpoint=ChunkEndpoint(id=b"hex1", span=(10, 20))
                )
            ]
        )
    ),
    # namespace Tree pointing to other namespaces
    (
        Tree(
            packed_payload=TreeData(
                type=NAMESPACE,
                mdata=[
                    LinkMeta(type=NAMESPACE, name="helloworld"),
                    LinkMeta(type=NAMESPACE, name="morestuff"),
                ]
            ),
            links=[b"hex0", b"hex1"]
        ),
        Tree.from_links_description(
            link_descriptors=[
                NamedLink(
                    name="helloworld", endpoint=NamespaceEndpoint(id=b"hex0")
                ),
                NamedLink(
                    name="morestuff", endpoint=NamespaceEndpoint(id=b"hex1")
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

