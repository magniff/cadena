from sqlalchemy import Column, ForeignKey, Integer, LargeBinary


from sqlalchemy.ext.declarative import declarative_base


from sqlalchemy.orm import relationship


BaseMapping = declarative_base()


class Link(BaseMapping):
    __tablename__ = "links"
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent = Column(LargeBinary(length=32), ForeignKey("nodes.id"), index=True)
    child = Column(LargeBinary(length=32), ForeignKey("nodes.id"), index=True)


class Node(BaseMapping):
    __tablename__ = "nodes"
    id = Column(LargeBinary(length=32), primary_key=True)
    data = Column(LargeBinary)
    links = relationship(Link, primaryjoin=Link.parent == id)

