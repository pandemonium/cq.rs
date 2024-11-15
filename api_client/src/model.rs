use serde::{Deserialize, Serialize};
use std::fmt;
use time::UtcOffset;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Author {
    pub id: AuthorId,
    pub info: AuthorInfo,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AuthorId(pub Uuid);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AuthorInfo {
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Book {
    pub id: BookId,
    pub info: BookInfo,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BookId(pub Uuid);

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reader {
    pub id: ReaderId,
    pub info: ReaderInfo,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ReaderId(pub Uuid);

impl fmt::Display for ReaderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(id) = self;
        write!(f, "{id}")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReaderInfo {
    pub name: String,
    pub unique_moniker: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BookRead {
    pub reader_id: ReaderId,
    pub book_id: BookId,
    pub when: Option<UtcOffset>,
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
