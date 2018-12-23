import py.test


from hypothesis import strategies as s
from hypothesis import given


from sqlalchemy import create_engine


import snapshot.drivers.memdict as memdict
import snapshot.drivers.alchemy as alchemy


Nodes = s.deferred(lambda: s.tuples(s.binary(min_size=0, max_size=5), Links))
Links = s.deferred(lambda: s.lists(Nodes, max_size=5, min_size=0))


def store(node, driver, **kwargs):
    data, links = node
    if not links:
        return driver.store(data=data, links=links, **kwargs)
    else:
        return driver.store(
            data=data,
            links=[
                store(link, driver, **kwargs) for link in links
            ],
            **kwargs
        )


def lookup(node_id, driver, **kwargs):
    node = driver.lookup(node_id=node_id, **kwargs)
    return (
        node.data,
        [lookup(link, driver, **kwargs) for link in node.links]
    )


@py.test.fixture(scope="function")
def sqlite_engine():
    yield create_engine("sqlite:///:memory:")


@py.test.mark.slow
@given(node=Nodes)
def test_memdict_driver(node):
    driver = memdict.MemdictDriver()
    assert node == lookup(store(node, driver), driver)


@py.test.mark.slow
@given(node=Nodes)
def test_alchemy_driver_no_session_reuse(node, sqlite_engine):
    """
    Here each IO action creates separate session scope.
    """

    driver = alchemy.AlchemyDriver(sqlite_engine)
    root_key = store(node, driver)
    assert node == lookup(root_key, driver)


@py.test.mark.slow
@given(node=Nodes)
def test_alchemy_driver_same_session_reuse(node, sqlite_engine):
    """
    The same IO session is shared between write and read actions
    """

    driver = alchemy.AlchemyDriver(sqlite_engine)
    with alchemy.driver.new_session(driver.db_engine) as session:
        root_key = store(node, driver, session=session)
        assert node == lookup(root_key, driver, session=session)


@py.test.mark.slow
@given(node=Nodes)
def test_alchemy_driver_session_reuse(node, sqlite_engine):
    """
    Write and read actions use separate sessions
    """

    driver = alchemy.AlchemyDriver(sqlite_engine)
    with alchemy.driver.new_session(driver.db_engine) as session:
        root_key = store(node, driver, session=session)
    with alchemy.driver.new_session(driver.db_engine) as session:
        assert node == lookup(root_key, driver, session=session)

