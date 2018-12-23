from contextlib import contextmanager


from sqlalchemy.orm import scoped_session, sessionmaker


from cadena.abc import AbstractDriver
from cadena.drivers.common import DefaultLinkedNode, sha256_node_id


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

        if self.lookup(node_id=snapshot_node.id, session=session):
            return snapshot_node.id

        node_data = Node(id=snapshot_node.id, data=snapshot_node.data)
        for link in links:
            node_data.links.append(Link(child=link))
        session.add(node_data)

        return snapshot_node.id

    @autosession
    def lookup(self, *, node_id, session=None):
        node_data_result = session.query(Node).filter_by(id=node_id).first()
        return (
            node_data_result and
            self.return_type.from_mapping(
                {
                    "data": node_data_result.data,
                    "links": [item.child for item in node_data_result.links]
                },
                id_maker=self.node_identifier
            )
        )

