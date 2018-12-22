from contextlib import contextmanager


from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import IntegrityError


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


class AlchemyDriver(AbstractDriver):

    def __init__(self, db_engine):
        # create db tables if no any
        BaseMapping.metadata.create_all(db_engine)

        self.return_type = DefaultLinkedNode
        self.node_identifier = sha256_node_id
        self.db_engine = db_engine

    def store(self, data, links=None):
        links = list() if links is None else links

        snapshot_node = self.return_type.from_mapping(
            id_maker=self.node_identifier,
            mapping={"data": data, "links": links}
        )

        if self.retrieve(snapshot_node.id):
            return snapshot_node.id

        with transaction(self.db_engine) as t:
            t.add(Node(id=snapshot_node.id, data=snapshot_node.data))
            for link in links:
                t.add(Link(parent=snapshot_node.id, child=link))

        return snapshot_node.id

    def retrieve(self, node_id):
        with transaction(self.db_engine) as t:
            node_data_result = t.query(Node).filter_by(id=node_id).first()
            return (
                node_data_result and
                self.return_type.from_mapping(
                    {
                        "data": node_data_result.data,
                        "links": [
                            item.child for item in
                            t.query(Link).filter_by(parent=node_id).all()
                        ]
                    },
                    id_maker=self.node_identifier
                )
            )

