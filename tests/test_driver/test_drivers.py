import py.test


from sqlalchemy import create_engine


import snapshot.drivers.memdict as memdict
import snapshot.drivers.alchemy as alchemy


@py.test.fixture(scope="function")
def db_engine(tmpdir):
    return create_engine("sqlite:///" + str(tmpdir.join("nodes.db")))


@py.test.fixture(
    scope="function",
    params=[
        lambda *args, **kwargs: memdict.MemdictDriver(),
        lambda db_engine: alchemy.AlchemyDriver(db_engine)
    ]
)
def driver(request, db_engine):
    return request.param(db_engine)


def test_uc0(driver):
    data_node_key = driver.store(b"huge_string_of_bytes", links=[])
    value0_key = driver.store(data=b"value0_data", links=[data_node_key])
    value1_key = driver.store(data=b"value1_data", links=[data_node_key])
    assert value0_key != value1_key

    value0 = driver.retrieve(value0_key)
    value1 = driver.retrieve(value1_key)

    assert value0.data == b"value0_data"
    assert value1.data == b"value1_data"
    assert value0.links == value1.links

