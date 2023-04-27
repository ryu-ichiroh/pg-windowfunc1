from sqlalchemy import Table
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Column
from sqlalchemy.types import Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import sessionmaker

engine = create_engine("postgresql://postgres:postgrespassword@localhost:5432/postgres", echo=True)


class Base(DeclarativeBase):
    pass

book_authors = Table(
    "book_authors",
    Base.metadata,
    Column("book_id", ForeignKey("books.book_id")),
    Column("author_id", ForeignKey("authors.author_id")),
    Column("position", Integer),
)

class Author(Base):
    __tablename__ = "authors"
    author_id = Column(UUID, primary_key=True)
    name = Column(Text)

class Book(Base):
    __tablename__ = "books"
    book_id = Column(UUID, primary_key=True)
    title = Column(Text)
    authors: Mapped[list[Author]] = relationship(secondary=book_authors, order_by=book_authors.c.position, lazy="selectin")

SessionClass = sessionmaker(engine)
session = SessionClass()

books = session.query(Book).filter(Book.book_id.in_(['1FB112D1-54C9-4308-99C6-0163BFD0172D', '554BC347-F36C-4766-B66F-D651C84C56BA'])).all()

print()
for book in books:
    print()
    print(book.book_id, book.title)
    print([(author.author_id, author.name) for author in book.authors])

# SELECT books.book_id AS books_book_id, books.title AS books_title
# FROM books
# WHERE books.book_id IN (%(book_id_1_1)s::UUID, %(book_id_1_2)s::UUID)

# lazy='select'
# SELECT authors.author_id AS authors_author_id, authors.name AS authors_name
# FROM authors, book_authors
# WHERE %(param_1)s::UUID = book_authors.book_id AND authors.author_id = book_authors.author_id

# lazy='joined'
# SELECT books.book_id AS books_book_id, books.title AS books_title, authors_1.author_id AS authors_1_author_id, authors_1.name AS authors_1_name
# FROM books LEFT OUTER JOIN (book_authors AS book_authors_1 JOIN authors AS authors_1 ON authors_1.author_id = book_authors_1.author_id) ON books.book_id = book_authors_1.book_id
# WHERE books.book_id IN (%(book_id_1_1)s::UUID, %(book_id_1_2)s::UUID)

# lazy='subquery'
# SELECT authors.author_id AS authors_author_id, authors.name AS authors_name, anon_1.books_book_id AS anon_1_books_book_id
# FROM (SELECT books.book_id AS books_book_id
#       FROM books
#       WHERE books.book_id IN (%(book_id_1_1)s::UUID, %(book_id_1_2)s::UUID)) AS anon_1 JOIN book_authors AS book_authors_1 ON anon_1.books_book_id = book_authors_1.book_id JOIN authors ON authors.author_id = book_authors_1.author_id

# lazy='selectin'
# SELECT books_1.book_id AS books_1_book_id, authors.author_id AS authors_author_id, authors.name AS authors_name
# FROM books AS books_1 JOIN book_authors AS book_authors_1 ON books_1.book_id = book_authors_1.book_id JOIN authors ON authors.author_id = book_authors_1.author_id
# WHERE books_1.book_id IN (%(primary_keys_1)s::UUID, %(primary_keys_2)s::UUID)

