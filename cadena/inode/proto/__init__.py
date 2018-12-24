from .inode_pb2 import InodePayload, Chunk, Tag


def dumps(payload_instance: InodePayload) -> bytes:
    return payload_instance.SerializeToString()


def loads(binary: bytes) -> InodePayload:
    return InodePayload.FromString(binary)


def chunk_payload(binary: bytes) -> InodePayload:
    return InodePayload(chunk=Chunk(data=binary, lenght=len(binary)))


def file_payload(name: str) -> InodePayload:
    return InodePayload(tag=Tag(value=name, type=Tag.FILE))


def folder_payload(name: str) -> InodePayload:
    return InodePayload(tag=Tag(value=name, type=Tag.FOLDER))


