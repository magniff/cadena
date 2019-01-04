from cadena.drivers.abc import AbstractDriver
from cadena.drivers.common import sha256_id


class MemdictDriver(AbstractDriver):

    def __init__(self):
        super().__init__(node_identifier=sha256_id)
        self.storage = dict()

    def store(self, node):
        node_id = self.node_identifier(node)
        self.storage[node_id] = node
        return node_id

    def lookup(self, node_id):
        return self.storage.get(node_id, None)

