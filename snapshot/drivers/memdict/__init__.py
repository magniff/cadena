from snapshot.abc import AbstractDriver
from snapshot.drivers.common import DefaultLinkedNode, sha256_node_id


class MemdictDriver(AbstractDriver):

    def __init__(self):
        self.storage = dict()
        self.return_type = DefaultLinkedNode
        self.node_identifier = sha256_node_id

    def __len__(self):
        return len(self.storage)

    def store(self, data, links=None):
        node_instance = self.return_type.from_mapping(
            {
                "data": data,
                "links": list() if links is None else links
            },
            id_maker=self.node_identifier
        )
        self.storage[node_instance.id] = node_instance
        return node_instance.id

    def lookup(self, node_id):
        return self.storage.get(node_id, None)

