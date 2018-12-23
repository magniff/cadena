from contextlib import contextmanager


from sqlalchemy.orm import scoped_session, sessionmaker


from snapshot.abc import AbstractDriver
from snapshot.drivers.common import DefaultLinkedNode, sha256_node_id


from .db_objects import BaseMapping, Node, Link


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
    """ Works with AlchemyDriver methods like "store" and "retreive".
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
        # create db tables if no any
        BaseMapping.metadata.create_all(db_engine)

        self.return_type = DefaultLinkedNode
        self.node_identifier = sha256_node_id
        self.db_engine = db_engine

    @autosession
    def store(self, *, data, links=None, session=None):
        links = list() if links is None else links
        snapshot_node = self.return_type.from_mapping(
            id_maker=self.node_identifier,
            mapping={"data": data, "links": links}
        )

        if self.retrieve(node_id=snapshot_node.id, session=session):
            return snapshot_node.id

        session.add(Node(id=snapshot_node.id, data=snapshot_node.data))
        for link in links:
            session.add(Link(parent=snapshot_node.id, child=link))

        return snapshot_node.id

    @autosession
    def retrieve(self, *, node_id, session=None, check=False):
        node_data_result = session.query(Node).filter_by(id=node_id).first()
        return (
            node_data_result and
            self.return_type.from_mapping(
                {
                    "data": node_data_result.data,
                    "links": [
                        item.child for item in
                        session.query(Link).filter_by(parent=node_id).all()
                    ]
                },
                id_maker=self.node_identifier
            )
        )
