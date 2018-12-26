import click
import pathlib


from cadena.drivers.alchemy.helpers import new_sqlite_driver_from_path
from cadena.dag import Commit, Blob, Tree


def store_dir(path, driver):
    tree = Tree.from_pairs(
        [
            (
                subpath.name, store_path(subpath, driver)
            )
            for subpath in path.iterdir()
        ]
    )
    return driver.store(node=tree.dump())


def store_file(path, driver):
    with open(str(path.absolute()), "rb") as f:
        blob_id = driver.store(node=Blob.from_data(data=f.read()).dump())
    return driver.store(
        node=Tree.from_pairs([(path.name, blob_id)]).dump()
    )


def store_path(path, driver):
    print(path.absolute())
    if path.is_file():
        return store_file(path, driver)
    else:
        return store_dir(path, driver)


@click.command()
@click.argument("path_to_store", type=click.Path(exists=True))
@click.option("--parrent", "-p", type=str, required=False)
def cli(path_to_store, parrent):
    driver = new_sqlite_driver_from_path("./storage.db")

    commit = Commit.from_tree_parents(
        tree=store_path(
            path=pathlib.Path(path_to_store),
            driver=driver,
        ),
        parents=[bytes.fromhex(parrent)] if parrent else []
    )
    click.echo(driver.store(node=commit.dump()).hex())


if __name__ == "__main__":
    cli()

