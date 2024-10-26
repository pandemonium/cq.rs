use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct Author {
    pub id: AuthorId,
    pub info: AuthorInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthorId(pub Uuid);

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthorInfo {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Book {
    pub id: BookId,
    pub info: BookInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BookId(pub Uuid);

#[derive(Debug, Serialize, Deserialize)]
pub struct BookInfo {
    pub isbn: String,
    pub title: String,
    pub author: AuthorId,
}

impl fmt::Display for BookId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(id) = self;
        write!(f, "{id}")
    }
}

impl fmt::Display for AuthorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(id) = self;
        write!(f, "{id}")
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResultItem {
    pub uri: String,
    pub hit: SearchHit,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SearchHit {
    #[serde(rename = "book-title")]
    BookTitle { title: String, id: BookId },
    #[serde(rename = "book-isbn")]
    BookIsbn { isbn: String, id: BookId },
    #[serde(rename = "author")]
    Author { name: String, id: AuthorId },
}
