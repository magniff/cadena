import hashlib
from snapshot.abc import AbstractHashingDriver, AbstractLinkedNode


def node_id_getter(node, content_identifier):
    id_object = content_identifier(node.data + b"".join(node.links))
    return id_object.digest()


class NodeImplementation(AbstractLinkedNode):

    @classmethod
    def from_mapping(cls, mapping, id_getter):
        instance = cls(
            data=mapping["data"], links=mapping["links"],

        )
        instance.id = instance.get_node_id(id_getter)
        return instance

    def get_node_id(self, id_getter):
        return id_getter(self)

    def __init__(self, data, links):
        self.data = data
        self.links = links


class MemdictDriver(AbstractHashingDriver):

    def __init__(self):
        self.storage = dict()
        self.return_type = NodeImplementation
        self.content_identifier = hashlib.sha256

    def __len__(self):
        return len(self.storage)

    def store(self, data, links=None):
        node_instance = self.return_type.from_mapping(
            {
                "data": data,
                "links": list() if links is None else links
            },
            id_getter=lambda node: node_id_getter(node, self.content_identifier)
        )
        self.storage[node_instance.id] = node_instance
        return node_instance.id

    def retrieve(self, node_id):
        return self.storage.get(node_id, None)

