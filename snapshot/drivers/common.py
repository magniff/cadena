import hashlib


from snapshot.abc import IdentifiedLinkedNode


def sha256_node_id(node):
    return hashlib.sha256(node.data + b"".join(node.links)).digest()


class DefaultLinkedNode(IdentifiedLinkedNode):

    def __repr__(self):
        return "%s<data=%s;links=%s>" % (
            type(self).__qualname__, len(self.data), len(self.links)
        )

    @classmethod
    def from_mapping(cls, mapping, id_maker):
        return cls(id_maker=id_maker, **mapping)


