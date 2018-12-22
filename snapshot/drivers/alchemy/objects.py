from sqlalchemy import Column, LargeBinary, Integer


from .base import BaseMapping


class Node(BaseMapping):
    __tablename__ = "nodes",
    id = Column(LargeBinary(length=32), primary_key=True)
    data = Column(LargeBinary)


class Link(BaseMapping):
    __tablename__ = "links"
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent = Column(LargeBinary(length=32), index=True)
    child = Column(LargeBinary(length=32))

