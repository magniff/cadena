import py.test
import snapshot.drivers.sqlite as sqlite


from sqlalchemy import create_engine


@py.test.fixture(scope="function")
def db_engine(tmpdir):
    return create_engine("sqlite:///" + str(tmpdir.join("nodes.db")))


@py.test.fixture(scope="function")
def sqlite_driver(db_engine):
    return sqlite.SqliteDriver(db_engine=db_engine)


def test_basic(sqlite_driver):

    storage_key = sqlite_driver.store(b"helloworld", links=[])
    retrieved_value = sqlite_driver.retrieve(storage_key)

    assert storage_key
    assert (
        retrieved_value.data == b"helloworld" and
        retrieved_value.links == []
    )

