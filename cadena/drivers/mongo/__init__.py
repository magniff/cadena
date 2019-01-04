import pymongo


from cadena.drivers.abc import AbstractDriver, DAGNode
from cadena.drivers.common import sha256_id


class MongoDriver(AbstractDriver):

    def __init__(self, host="localhost", port=27017):
        super().__init__(node_identifier=sha256_id)
        self.storage = pymongo.MongoClient(host, port).cadena_db.cadena_data

    def store(self, node, *args, **kwargs):
        node_id = self.node_identifier(node)
        if self.storage.find_one({"id": node_id.hex()}):
            return node_id

        self.storage.insert_one(
            {
                "id": node_id.hex(),
                "data": node.data.hex(),
                "links": [link.hex() for link in node.links]
            }
        )

        return node_id

    def lookup(self, node_id, *args, **kwargs):
        result = self.storage.find_one({"id": node_id.hex()})
        return (
            result and
            DAGNode(
                data=bytes.fromhex(result["data"]),
                links=[
                    bytes.fromhex(link) for link in result["links"]
                ]
            )
        )

