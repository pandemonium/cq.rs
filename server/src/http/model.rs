use serde::{Deserialize, Serialize};
use std::fmt;
use time::OffsetDateTime;

use crate::core::model as domain;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KeywordTarget {
    Book { book_id: BookId },
    Author { author_id: AuthorId },
}

impl From<domain::KeywordTarget> for KeywordTarget {
    fn from(value: domain::KeywordTarget) -> Self {
        match value {
            domain::KeywordTarget::Book(book_id) => Self::Book {
                book_id: book_id.into(),
            },
            domain::KeywordTarget::Author(author_id) => Self::Author {
                author_id: author_id.into(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct AuthorId(pub domain::AuthorId);

impl fmt::Display for AuthorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(domain::AuthorId(id)) = self;
        write!(f, "{id}")
    }
}

impl From<domain::AuthorId> for AuthorId {
    fn from(value: domain::AuthorId) -> Self {
        Self(value)
    }
}

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

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct BookId(pub domain::BookId);

impl fmt::Display for BookId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(domain::BookId(id)) = self;
        write!(f, "{id}")
    }
}

impl From<domain::BookId> for BookId {
    fn from(value: domain::BookId) -> Self {
        Self(value)
    }
}

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

#[derive(Serialize, Deserialize)]
pub struct Reader {
    id: domain::ReaderId,
    info: domain::ReaderInfo,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct ReaderId(pub domain::ReaderId);

impl From<ReaderId> for domain::ReaderId {
    fn from(ReaderId(value): ReaderId) -> Self {
        value
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NewReader(pub domain::ReaderInfo);

impl fmt::Display for ReaderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(domain::ReaderId(id)) = self;
        write!(f, "{id}")
    }
}

impl From<domain::Reader> for Reader {
    fn from(domain::Reader(id, info): domain::Reader) -> Self {
        Self { id, info }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NewBookRead {
    pub reader_id: ReaderId,
    pub when: Option<OffsetDateTime>,
}

#[derive(Deserialize)]
pub struct SearchTerm {
    pub query: String,
}

// This should be in the core model, but then I would
// have to refashion this here
#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResultItem {
    uri: String,
    // This can use a peer to text_model::Projection instead
    // because it can also become a Resource.
    hit: SearchHit,
}

impl SearchResultItem {
    pub fn from_search_hit(hit: SearchHit, resource_prefix: &str) -> Self {
        Self {
            uri: hit.referenced_resource().uri(resource_prefix),
            hit,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SearchHit {
    BookTitle { title: String, id: BookId },
    BookIsbn { isbn: String, id: BookId },
    Author { name: String, id: AuthorId },
}

impl SearchHit {
    fn referenced_resource(&self) -> Resource {
        match self {
            SearchHit::BookTitle { id, .. } => Resource::Book(*id),
            SearchHit::BookIsbn { id, .. } => Resource::Book(*id),
            SearchHit::Author { id, .. } => Resource::Author(*id),
        }
    }
}

use domain::query::text as text_search;
impl From<text_search::SearchHit> for SearchHit {
    fn from(text_search::SearchHit { target, source }: text_search::SearchHit) -> Self {
        match target {
            text_search::Projection::Books(text_search::BookField::Isbn(id)) => Self::BookIsbn {
                isbn: source,
                id: id.into(),
            },
            text_search::Projection::Books(text_search::BookField::Title(id)) => Self::BookTitle {
                title: source,
                id: id.into(),
            },
            text_search::Projection::Authors(text_search::AuthorField::Name(id)) => Self::Author {
                name: source,
                id: id.into(),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Resource {
    Author(AuthorId),
    Book(BookId),
}

impl Resource {
    fn uri(&self, prefix: &str) -> String {
        match self {
            Resource::Author(id) => format!("{prefix}/authors/{id}"),
            Resource::Book(id) => format!("{prefix}/books/{id}"),
        }
    }
}

impl From<text_search::Projection> for Resource {
    fn from(value: text_search::Projection) -> Self {
        match value {
            text_search::Projection::Books(text_search::BookField::Isbn(id)) => {
                Resource::Book(id.into())
            }
            text_search::Projection::Books(text_search::BookField::Title(id)) => {
                Resource::Book(id.into())
            }
            text_search::Projection::Authors(text_search::AuthorField::Name(id)) => {
                Resource::Author(id.into())
            }
        }
    }
}
