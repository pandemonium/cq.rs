use serde::{Deserialize, Serialize};

use crate::core::model as domain;

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthorId(pub domain::AuthorId);

#[derive(Serialize, Deserialize)]
pub struct Author {
    id: domain::AuthorId,
    info: domain::AuthorInfo,
}

impl From<domain::Author> for Author {
    fn from(domain::Author(id, info): domain::Author) -> Self {
        Self { id, info }
    }
}

impl From<Author> for domain::Author {
    fn from(Author { id, info }: Author) -> Self {
        Self(id, info)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BookId(pub domain::BookId);

#[derive(Debug, Serialize, Deserialize)]
pub struct Book {
    id: domain::BookId,
    info: domain::BookInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NewBook(pub domain::BookInfo);

impl From<domain::Book> for Book {
    fn from(domain::Book(id, info): domain::Book) -> Self {
        Self { id, info }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NewAuthor(pub domain::AuthorInfo);
