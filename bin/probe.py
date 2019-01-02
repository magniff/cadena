import json
import base64
import sys


import click


from cadena.drivers.alchemy.helpers import new_sqlite_driver_from_path
from cadena.fslike_dag import Commit, Blob, Tree, load_from_dagnode, NAMESPACE


def dump_node_stats(node):
    node_dict = {"type": type(node).__qualname__.upper()}

    if isinstance(node, Commit):
        node_dict["parents"] = [parent.hex() for parent in node.parents]
        node_dict["tree"] = node.tree.hex()

    elif isinstance(node, Tree):
        node_dict["type"] = (
            "NAMESPACE" if node.type == NAMESPACE else "DATA"
        )
        node_dict["subtrees"] = [
            {
                "id": link.hex(),
                "name": link_meta.name,
                "span": (link_meta.span_from, link_meta.span_to),
                "type": "NAMESPACE" if link_meta.type == NAMESPACE else "DATA"
            }
            for link_meta, link in zip(node.packed_payload.mdata, node.links)
        ]

    elif isinstance(node, Blob):
        node_dict["data"] = base64.b64encode(node.bytes).decode()

    return node_dict


@click.command()
@click.option("--db_path", type=click.Path(exists=True), default="snapshot.db")
@click.argument("tree_id", type=str)
def cli(db_path, tree_id):
    # try to parse tree_id
    try:
        tree_id_native = bytes.fromhex(tree_id)
    except ValueError:
        click.echo(
            "Fatal: %s is not a valid tree id." % tree_id,
            err=True, color="red"
        )
        sys.exit(1)

    driver = new_sqlite_driver_from_path(db_path)
    maybe_tree = load_from_dagnode(driver.lookup(node_id=tree_id_native))
    if maybe_tree is not None:
        click.echo(
            json.dumps(dump_node_stats(maybe_tree), indent=4, sort_keys=True)
        )


if __name__ == "__main__":
    cli()

