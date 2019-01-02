import click
import pathlib


from cadena.drivers.alchemy.helpers import (
    new_sqlite_driver_from_path, new_session_from_driver
)


from cadena.fslike_dag import (
    Blob, Tree, NamedLink, ChunkEndpoint, NamespaceEndpoint
)


def store_dir(path, driver, session):
    tree = Tree.from_links_description(
        link_descriptors=[
            NamedLink(
                name=subpath.name,
                endpoint=NamespaceEndpoint(
                    id=store_path(subpath, driver, session)
                )
            )
            for subpath in path.iterdir()
        ]
    )
    return driver.store(node=tree.dump(), session=session)


def store_file(path, driver, session, self_namespace=False):
    with open(str(path.absolute()), "rb") as f:
        binary_data = f.read()
        blob_id = driver.store(
            node=Blob.from_data(data=binary_data).dump(),
            session=session
        )

    if not self_namespace:
        id_to_return = blob_id
    else:
        id_to_return = driver.store(
            node=Tree.from_links_description(
                link_descriptors=[
                    NamedLink(
                        name=path.name,
                        endpoint=ChunkEndpoint(
                            id=blob_id, span=(0, len(binary_data))
                        )
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

