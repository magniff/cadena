from sqlalchemy import create_engine


from .driver import AlchemyDriver, new_session


def new_session_from_driver(driver):
    return new_session(driver.db_engine)


def new_sqlite_driver_from_path(path):
    return AlchemyDriver(db_engine=create_engine("sqlite:///%s" % path))


def new_sqlite_inmem_driver():
    return AlchemyDriver(db_engine=create_engine("sqlite:///:memory:"))

