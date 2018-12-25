import hashlib


from cadena.abc import IdentifiedDAGNode


def sha256_id(data, links):
    return hashlib.sha256(data + b"".join(links)).digest()


def sha256_node_id(node):
    return hashlib.sha256(node.data + b"".join(node.links)).digest()


class DefaultDAGNode(IdentifiedDAGNode):

    def __repr__(self):
        return "%s<data=%s;links=%s>" % (
            type(self).__qualname__, len(self.data), len(self.links)
        )

    @classmethod
    def from_mapping(cls, mapping, id_maker):
        return cls(id_maker=id_maker, **mapping)

    def to_mapping(self):
        return {"data": self.data, "links": self.links}

