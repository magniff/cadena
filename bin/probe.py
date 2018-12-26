import json
import base64


import click


from cadena.drivers.alchemy.helpers import new_sqlite_driver_from_path
from cadena.dag import Commit, Blob, Tree, load_from_dagnode


def dump_node_stats(node):
    node_dict = {"type": type(node).__qualname__}

    if isinstance(node, Commit):
        node_dict["parents"] = [parent.hex() for parent in node.parents]
        node_dict["tree"] = node.tree.hex()
    elif isinstance(node, Tree):
        node_dict["subtrees"] = {
            name: link.hex() for
            name, link in zip(node.packed_payload.names, node.links)
        }
    elif isinstance(node, Blob):
        node_dict["data"] = base64.urlsafe_b64encode(node.bytes).decode()

    return node_dict


@click.command()
@click.option("--db_path", type=click.Path(exists=True), default="./storage.db")
@click.argument("commit", type=str)
def cli(db_path, commit):
    driver = new_sqlite_driver_from_path(db_path)
    some_node = load_from_dagnode(driver.lookup(node_id=bytes.fromhex(commit)))

    if some_node is not None:
        click.echo(
            json.dumps(dump_node_stats(some_node), indent=4, sort_keys=True)
        )


if __name__ == "__main__":
    cli()

