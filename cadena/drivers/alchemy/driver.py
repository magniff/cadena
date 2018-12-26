from contextlib import contextmanager


from sqlalchemy.orm import scoped_session, sessionmaker


from cadena.abc import AbstractDriver, DAGNode
from cadena.drivers.common import sha256_id


from .db_objects import BaseMapping, Node as DBNode, Link as DBLink


@contextmanager
def new_session(db_engine):
    session = scoped_session(sessionmaker(bind=db_engine))
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def autosession(method):
    """ Works with AlchemyDriver methods like "store" and "lookup".
    """

    def decorator(*args, **kwargs):
        self, *_ = args

        session = kwargs.get("session")
        if session is None:
            with new_session(self.db_engine) as s:
                kwargs["session"] = s
                return method(*args, **kwargs)
        return method(*args, **kwargs)

    return decorator


class AlchemyDriver(AbstractDriver):

    def __init__(self, db_engine):
        super().__init__(node_identifier=sha256_id)
        self.db_engine = db_engine
        BaseMapping.metadata.create_all(self.db_engine)

    @autosession
    def store(self, *, node, session=None):
        id_to_return = sha256_id(node)
        if self.lookup(node_id=id_to_return, session=session) is not None:
            return id_to_return

        session.add(
            DBNode(
                id=id_to_return,
                data=node.data,
                links=[
                    DBLink(child=link) for link in node.links
                ]
            )
        )

        return id_to_return

    @autosession
    def lookup(self, *, node_id, session=None):
        db_node = session.query(DBNode).filter_by(id=node_id).first()
        return (
            db_node and
            DAGNode(
                data=db_node.data,
                links=[
                    item.child for item in db_node.links
                ]
            )
        )

