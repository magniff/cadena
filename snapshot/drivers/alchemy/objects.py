from sqlalchemy import (
    Table, Column, LargeBinary, PrimaryKeyConstraint
)


from .base import BaseMapping


class Node(BaseMapping):
    __tablename__ = "nodes",
    id = Column(LargeBinary(length=32), primary_key=True)
    data = Column(LargeBinary)


class Link(BaseMapping):
    __tablename__ = "links"
    parent = Column(LargeBinary(length=32), primary_key=True)
    child = Column(LargeBinary(length=32), primary_key=True)

