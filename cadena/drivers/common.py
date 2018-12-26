import hashlib


def sha256_id(node):
    return hashlib.sha256(node.data + b"".join(node.links)).digest()

