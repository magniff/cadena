import click
import pathlib


from cadena.drivers.alchemy.helpers import (
    new_sqlite_driver_from_path, new_session_from_driver
)


from cadena.dag import Blob, Tree, LinkDescriptor, DATA, NAMESPACE


def store_dir(path, driver, session):
    tree = Tree.from_description(
        tree_type=NAMESPACE,
        link_descriptors=[
            LinkDescriptor(
                link_type=NAMESPACE if subpath.is_dir() else DATA,
                name=subpath.name,
                endpoint=store_path(subpath, driver, session)
            )
            for subpath in path.iterdir()
        ]
    )
    return driver.store(node=tree.dump(), session=session)


def store_file(path, driver, session, self_namespace=False):
    with open(str(path.absolute()), "rb") as f:
        blob_id = driver.store(
            node=Blob.from_data(data=f.read()).dump(),
            session=session
        )

    if not self_namespace:
        id_to_return = blob_id
    else:
        id_to_return = driver.store(
            node=Tree.from_description(
                tree_type=NAMESPACE,
                link_descriptors=[
                    LinkDescriptor(
                        endpoint=blob_id, name=path.name, link_type=DATA
                    )
                ]
            ).dump(),
            session=session
        )
    return id_to_return


def store_path(path, driver, session):
    if path.is_file():
        return store_file(path, driver, session)
    else:
        return store_dir(path, driver, session)


@click.command()
@click.argument("path_to_store", type=click.Path(exists=True))
@click.option("--parent", "-p", type=str, required=False)
def cli(path_to_store, parent):
    driver = new_sqlite_driver_from_path("snapshot.db")
    path = pathlib.Path(path_to_store)
    with new_session_from_driver(driver) as session:
        if path.is_file():
            root_id = store_file(path, driver, session, self_namespace=True)
        else:
            root_id = store_path(path=path, driver=driver, session=session)
        click.echo(root_id.hex())


if __name__ == "__main__":
    cli()

