import datetime
from typing import List

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.event import listens_for
from sqlalchemy import Column, ForeignKey, String, Integer, BLOB, JSON, DateTime
from sqlalchemy.dialects.postgresql import BYTEA, JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.ext.mutable import MutableDict
Base = declarative_base()

class WithTimestamps:
    created_date = Column(DateTime, nullable=False, default=lambda: datetime.datetime.now())

class Node(WithTimestamps, Base):
    __tablename__ = 'node'
    id = Column('node_id', Integer, primary_key=True)
    filename = Column(String, nullable=False)
    stored_path = Column(String, nullable=False)
    parent_id = Column(Integer, ForeignKey('node.node_id'))
    parent: 'Node' = relationship('Node', back_populates="children", remote_side=[id])
    type = Column(String)
    @property
    def calculated_path(self):
        return ((self.parent.calculated_path + '/' + self.filename) if self.parent else self.filename).lstrip('/')

    @property
    def content(self):
        return None

    def serialize(self, content: bool):
        return dict(
            type=self.type,
            name=self.filename,
            path= self.calculated_path,
            #
            mimetype=None,
            writable=True,
            created=1,
            last_modified=1,
            #
            format='json' if content else None,
            content=self.content if content else None
        )

    __mapper_args__ = {
        'polymorphic_on': 'type',
        'polymorphic_identity':'node'
    }

Node.children = relationship('Node', order_by=Node.filename, back_populates='parent')


@listens_for(Node, 'before_insert',propagate=True)
@listens_for(Node, 'before_update',propagate=True)
def node_before_update(mapper, connection, target: Node):
    target.stored_path = target.calculated_path

class Directory(Node):
    __tablename__ = 'directory'
    id = Column('directory_id',  Integer,ForeignKey('node.node_id'), primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity':'directory'
    }

    @property
    def content(self):
        return [_.serialize(content=False) for _ in self.children]


class File(Node):
    __tablename__ = 'file'
    id = Column('file_id',  Integer,ForeignKey('node.node_id'), primary_key=True)
    data = Column(BLOB().with_variant(BYTEA, 'postgresql'))

    __mapper_args__ = {
        'polymorphic_identity':'file'
    }
    @property
    def content(self):
        return self.data



class Notebook(Node):
    __tablename__ = 'notebook'
    id = Column('notebook_id',  Integer,ForeignKey('node.node_id'), primary_key=True)
    data = Column(JSON().with_variant(JSONB, 'postgresql'), nullable=False)

    __mapper_args__ = {
        'polymorphic_identity':'notebook'
    }
    @property
    def content(self):
        return self.data


