drop view  if exists author_rating;
drop view  if exists average_rating_by_category;
drop view  if exists percent_of_books_by_category;



drop table if exists book_author;
drop table if exists author;
drop  table if exists book;
drop table if exists format;
drop table if exists category;




CREATE TABLE author
(
  author_id INT NOT NULL,
  name VARCHAR NOT NULL,
  birthday_date DATE,
  PRIMARY KEY (author_id)
);


CREATE TABLE format
(
  format_id INT NOT NULL,
  name VARCHAR NOT NULL,
  PRIMARY KEY (format_id),
  UNIQUE (name)
);

CREATE TABLE category
(
  category_id INT NOT NULL,
  name VARCHAR NOT NULL,
  PRIMARY KEY (category_id),
  UNIQUE (name)
);

CREATE TABLE book
(
  name VARCHAR NOT NULL,
  isbn VARCHAR NOT NULL,
  rating INT NOT NULL,
  book_id INT NOT NULL,
  format_id INT NOT NULL,
  category_id INT NOT NULL,
  PRIMARY KEY (book_id),
  FOREIGN KEY (format_id) REFERENCES format(format_id),
  FOREIGN KEY (category_id) REFERENCES category(category_id),
  UNIQUE (isbn)
);


CREATE TABLE book_author
(
  book_id INT NOT NULL,
  author_id INT NOT NULL,
  PRIMARY KEY (book_id, author_id),
  FOREIGN KEY (book_id) REFERENCES book(book_id),
  FOREIGN KEY (author_id) REFERENCES author(author_id)
);