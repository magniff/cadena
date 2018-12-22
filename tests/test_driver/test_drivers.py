import py.test


from hypothesis import strategies as s
from hypothesis import given


from sqlalchemy import create_engine


import snapshot.drivers.memdict as memdict
import snapshot.drivers.alchemy as alchemy


Nodes = s.deferred(lambda: s.tuples(s.binary(min_size=0, max_size=10), Links))
Links = s.deferred(lambda: s.lists(Nodes, max_size=5, min_size=0))


def traverse(node, driver):
    data, links = node
    if not links:
        return driver.store(data=data, links=[])
    else:
        return driver.store(
            data=data, links=[traverse(link, driver) for link in links]
        )


def traverse_back(node_id, driver):
    node = driver.retrieve(node_id)
    return (node.data, [traverse_back(link, driver) for link in node.links])


@py.test.fixture(scope="function")
def db_engine(tmpdir):
    yield create_engine("sqlite:///" + str(tmpdir.join("nodes.db")))


@py.test.fixture(
    scope="function",
    params=[
        lambda *args, **kwargs: memdict.MemdictDriver(),
        lambda db_engine: alchemy.AlchemyDriver(db_engine)
    ]
)
def driver(request, db_engine):
    return request.param(db_engine)


@given(node=Nodes)
def test_basic_io(driver, node):
    assert node == traverse_back(traverse(node, driver), driver)

