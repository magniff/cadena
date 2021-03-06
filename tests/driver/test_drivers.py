import py.test


from hypothesis import strategies as s
from hypothesis import given
from hypothesis import settings, HealthCheck


from sqlalchemy import create_engine


import cadena.drivers.memdict as memdict
import cadena.drivers.alchemy as alchemy
import cadena.drivers.mongo as mongo


from cadena.drivers.abc import DAGNode


Nodes = s.deferred(lambda: s.tuples(s.binary(min_size=0, max_size=10), Links))
Links = s.deferred(lambda: s.lists(Nodes, max_size=3, min_size=0))


def store(pair, driver, **kwargs):
    data, links = pair

    if not links:
        return driver.store(node=DAGNode(data=data, links=links), **kwargs)
    else:
        return driver.store(
            node=DAGNode(
                data=data,
                links=[
                    store(link, driver, **kwargs) for link in links
                ]
            ),
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


@py.test.mark.external
@py.test.mark.slow
@given(node=Nodes)
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_mongo_driver(node):
    driver = mongo.MongoDriver()
    assert node == lookup(store(node, driver), driver)


@py.test.mark.slow
@given(node=Nodes)
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_memdict_driver(node):
    driver = memdict.MemdictDriver()
    assert node == lookup(store(node, driver), driver)


@py.test.mark.slow
@given(node=Nodes)
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_alchemy_driver_no_session_reuse(node, sqlite_engine):
    """
    Here each IO action creates separate session scope.
    """

    driver = alchemy.AlchemyDriver(sqlite_engine)
    root_key = store(node, driver)
    assert node == lookup(root_key, driver)


@py.test.mark.slow
@given(node=Nodes)
@settings(suppress_health_check=[HealthCheck.too_slow])
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
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_alchemy_driver_session_reuse(node, sqlite_engine):
    """
    Write and read actions use separate sessions
    """

    driver = alchemy.AlchemyDriver(sqlite_engine)
    with alchemy.driver.new_session(driver.db_engine) as session:
        root_key = store(node, driver, session=session)
    with alchemy.driver.new_session(driver.db_engine) as session:
        assert node == lookup(root_key, driver, session=session)

