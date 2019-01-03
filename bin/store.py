import click

import collections
import pathlib


from cadena.drivers.alchemy.helpers import (
    new_sqlite_driver_from_path, new_session_from_driver
)


from cadena.fslike_dag import (
    Blob, Tree, NamedLink, UnnamedLink, ChunkEndpoint, NamespaceEndpoint,
)


ChunkStorageResult = collections.namedtuple(
    "ChunkStorageResult", "id left right"
)
NamespaceStorageResult = collections.namedtuple("NamespaceStorageResult", "id")


# global chunk size value in KiB
CHUNK_SIZE = None


def endpoint_from_store_result(result):
    if isinstance(result, NamespaceStorageResult):
        endpoint = NamespaceEndpoint(id=result.id)
    else:
        endpoint = ChunkEndpoint(
            id=result.id, span=(result.left, result.right)
        )
    return endpoint


def store_dir(path, driver, session):
    # actual storage action performed here
    tree = Tree.from_links_description(
        link_descriptors=[
            NamedLink(
                name=subpath.name,
                endpoint=endpoint_from_store_result(
                    store_path(subpath, driver, session)
                )
            )
            for subpath in path.iterdir()
        ]
    )

    return NamespaceStorageResult(
        id=driver.store(node=tree.dump(), session=session)
    )


def store_file(path, driver, session, self_namespace=False):
    stored_chunks = list()

    with open(str(path.absolute()), "rb") as file_handler:
        left = right = 0
        binary_blob = file_handler.read(CHUNK_SIZE)

        while binary_blob:
            right = file_handler.tell()
            stored_chunks.append(
                ChunkStorageResult(
                    id=driver.store(
                        node=Blob.from_data(data=binary_blob).dump(),
                        session=session
                    ),
                    right=right,
                    left=left,
                )
            )
            left = right
            binary_blob = file_handler.read(CHUNK_SIZE)

    data_tree_id = driver.store(
        node=Tree.from_links_description(
            link_descriptors=[
                UnnamedLink(
                    endpoint=ChunkEndpoint(
                        id=chunk.id, span=(chunk.left, chunk.right)
                    )
                )
                for chunk in stored_chunks
            ]
        ).dump(),
        session=session
    )

    if not self_namespace:
        result_to_return = ChunkStorageResult(
            id=data_tree_id, right=right, left=0
        )
    else:
        result_to_return = NamespaceStorageResult(
            id=driver.store(
                node=Tree.from_links_description(
                    link_descriptors=[
                        NamedLink(
                            name=path.name,
                            endpoint=ChunkEndpoint(
                                id=data_tree_id, span=(0, right)
                            )
                        )
                    ]
                ).dump(),
                session=session
            )
        )

    return result_to_return


def store_path(path, driver, session):
    if path.is_file():
        return store_file(path, driver, session)
    else:
        return store_dir(path, driver, session)


@click.command()
@click.argument("path_to_store", type=click.Path(exists=True))
@click.option(
    "-s", "--chunk_size", type=int, required=False, default=1000
)
def cli(path_to_store, chunk_size):
    global CHUNK_SIZE
    CHUNK_SIZE = chunk_size * 1024

    driver = new_sqlite_driver_from_path("snapshot.db")
    path = pathlib.Path(path_to_store)

    with new_session_from_driver(driver) as session:
        if path.is_file():
            root_id = store_file(path, driver, session, self_namespace=True).id
        else:
            root_id = store_path(path=path, driver=driver, session=session).id
        click.echo(root_id.hex())


if __name__ == "__main__":
    cli()

