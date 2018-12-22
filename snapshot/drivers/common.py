import hashlib


from snapshot.abc import IdentifiedLinkedNode


def sha256_node_id(node):
    return hashlib.sha256(node.data + b"".join(node.links)).digest()


class DefaultLinkedNode(IdentifiedLinkedNode):

    @classmethod
    def from_mapping(cls, mapping, id_maker):
        return cls(
            id_maker=id_maker, **mapping

        )


