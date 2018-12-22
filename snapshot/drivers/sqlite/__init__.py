from sqlalchemy.orm import scoped_session, sessionmaker
from contextlib import contextmanager


from snapshot.abc import AbstractDriver
from snapshot.drivers.common import DefaultLinkedNode, sha256_node_id


from .base import BaseMapping
from .objects import Node, Link


@contextmanager
def transaction(db_engine):
    session = scoped_session(sessionmaker(bind=db_engine))
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


class SqliteDriver(AbstractDriver):

    def __init__(self, db_engine):
        self.return_type = DefaultLinkedNode
        self.node_identifier = sha256_node_id
        self.db_engine = db_engine
        BaseMapping.metadata.create_all(db_engine)

    def store(self, data, links=None):
        links = list() if links is None else links

        snapshot_node = DefaultLinkedNode.from_mapping(
            id_maker=self.node_identifier,
            mapping={"data": data, "links": links}
        )

        with transaction(self.db_engine) as t:
            t.add(Node(id=snapshot_node.id, data=snapshot_node.data))
            for link in links:
                t.add(Link(parent=snapshot_node.id, child=link))

        return snapshot_node.id

    def retrieve(self, node_id):
        with transaction(self.db_engine) as t:
            node_data_result = t.query(Node).filter_by(id=node_id).first()
            node_links_result = t.query(Link).filter_by(parent=node_id).all()
            return DefaultLinkedNode.from_mapping(
                {
                    "data": node_data_result.data,
                    "links": [item.child for item in node_links_result]
                },
                id_maker=self.node_identifier
            )

