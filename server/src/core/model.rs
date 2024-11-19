use core::fmt;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{str::FromStr, sync::OnceLock, time::SystemTime};
use time::OffsetDateTime;

use crate::{
    error::{Error, Result},
    infrastructure::{
        AggregateIdentity, AggregateRoot, AggregateStream, EventDescriptor, ExternalRepresentation,
        UniqueId,
    },
};

pub mod query;

#[derive(Clone, Debug)]
pub enum Event {
    BookAdded(BookId, BookInfo),
    AuthorAdded(AuthorId, AuthorInfo),
    ReaderAdded(ReaderId, ReaderInfo),
    BookRead(ReaderId, BookReadInfo),
    KeywordAdded(KeywordTarget, String),
}

impl Event {
    const BOOK_ADDED: &str = "book-added";
    const AUTHOR_ADDED: &str = "author-added";
    const READER_ADDED: &str = "reader-added";
    const BOOK_READ: &str = "book-read";
    const KEYWORD_ADDED: &str = "keyword-added";

    fn name(&self) -> &str {
        match self {
            Event::BookAdded(..) => Self::BOOK_ADDED,
            Event::AuthorAdded(..) => Self::AUTHOR_ADDED,
            Event::ReaderAdded(..) => Self::READER_ADDED,
            Event::BookRead(..) => Self::BOOK_READ,
            Event::KeywordAdded(..) => Self::KEYWORD_ADDED,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct KeywordAddedSurrogate {
    keyword: String,
    target: KeywordTarget,
}

impl EventDescriptor for Event {
    fn external_representation(
        &self,
        UniqueId(id): UniqueId,
        when: SystemTime,
    ) -> Result<ExternalRepresentation> {
        match self {
            Event::BookAdded(BookId(UniqueId(aggregate_id)), info) => Ok(ExternalRepresentation {
                id,
                when,
                aggregate_id: *aggregate_id,
                what: self.name().to_owned(),
                data: serde_json::to_value(info)?,
            }),
            Event::AuthorAdded(AuthorId(UniqueId(aggregate_id)), info) => {
                Ok(ExternalRepresentation {
                    id,
                    when,
                    aggregate_id: *aggregate_id,
                    what: self.name().to_owned(),
                    data: serde_json::to_value(info)?,
                })
            }
            Event::ReaderAdded(ReaderId(UniqueId(aggregate_id)), info) => {
                Ok(ExternalRepresentation {
                    id,
                    when,
                    aggregate_id: *aggregate_id,
                    what: self.name().to_owned(),
                    data: serde_json::to_value(info)?,
                })
            }
            Event::BookRead(ReaderId(UniqueId(aggregate_id)), info) => Ok(ExternalRepresentation {
                id,
                when,
                aggregate_id: *aggregate_id,
                what: self.name().to_owned(),
                data: serde_json::to_value(info)?,
            }),
            Event::KeywordAdded(target, keyword) => Ok(ExternalRepresentation {
                id,
                when,
                aggregate_id: *target.aggregate_id().uuid(),
                what: self.name().to_owned(),
                data: serde_json::to_value(KeywordAddedSurrogate {
                    keyword: keyword.to_owned(),
                    target: *target,
                })?,
            }),
        }
    }

    fn from_external_representation(
        ExternalRepresentation {
            aggregate_id,
            what,
            data,
            ..
        }: &ExternalRepresentation,
    ) -> Result<Self> {
        match what.as_str() {
            Event::AUTHOR_ADDED => Ok(Event::AuthorAdded(
                AuthorId(UniqueId(*aggregate_id)),
                serde_json::from_value(data.clone())?,
            )),
            Event::BOOK_ADDED => Ok(Event::BookAdded(
                BookId(UniqueId(*aggregate_id)),
                serde_json::from_value(data.clone())?,
            )),
            Event::READER_ADDED => Ok(Event::ReaderAdded(
                ReaderId(UniqueId(*aggregate_id)),
                serde_json::from_value(data.clone())?,
            )),
            Event::BOOK_READ => Ok(Event::BookRead(
                ReaderId(UniqueId(*aggregate_id)),
                serde_json::from_value(data.clone())?,
            )),
            Event::KEYWORD_ADDED => {
                let KeywordAddedSurrogate { keyword, target }: KeywordAddedSurrogate =
                    serde_json::from_value(data.clone())?;
                Ok(Event::KeywordAdded(target, keyword))
            }
            otherwise => Err(Error::UnknownEventType(otherwise.to_owned())),
        }
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeywordTarget {
    Book(BookId),
    Author(AuthorId),
}

impl KeywordTarget {
    fn aggregate_id(&self) -> &UniqueId {
        match self {
            KeywordTarget::Book(BookId(book_id)) => book_id,
            KeywordTarget::Author(AuthorId(author_id)) => author_id,
        }
    }
}

#[derive(Debug)]
pub struct Author(pub AuthorId, pub AuthorInfo);

#[derive(Debug)]
pub struct Book(pub BookId, pub BookInfo);

#[derive(Clone)]
pub enum Command {
    AddBook(BookInfo),
    AddAuthor(AuthorInfo),
    AddReader(ReaderInfo),
    AddReadBook(BookReadInfo),
    AddKeyword(Keyword, KeywordTarget),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Keyword(String);

impl Keyword {
    pub fn into_string(self) -> String {
        let Self(string) = self;
        string
    }
}

impl AsRef<str> for Keyword {
    fn as_ref(&self) -> &str {
        let Self(inner) = self;
        &inner
    }
}

impl fmt::Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(keyword) = self;
        write!(f, "{}", keyword)
    }
}

static KEYWORD_REGEX: OnceLock<Regex> = OnceLock::new();

fn keyword_regex() -> &'static Regex {
    KEYWORD_REGEX.get_or_init(|| Regex::new(r"[\p{L}_-]+").expect("KEYWORD_REGEX is valid"))
}

impl FromStr for Keyword {
    type Err = Error;

    fn from_str(keyword: &str) -> Result<Self> {
        if keyword_regex().is_match(keyword) {
            Ok(Self(keyword.to_owned()))
        } else {
            Err(Error::Generic(format!(
                "{keyword} is not a valid keyword name"
            )))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BookInfo {
    pub isbn: Isbn,
    pub title: String,
    pub author: AuthorId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthorInfo {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Isbn(pub String);

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorId(pub UniqueId);

impl From<AuthorId> for uuid::Uuid {
    fn from(AuthorId(UniqueId(id)): AuthorId) -> Self {
        id
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReaderId(pub UniqueId);

impl From<ReaderId> for uuid::Uuid {
    fn from(ReaderId(UniqueId(id)): ReaderId) -> Self {
        id
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReaderInfo {
    pub name: String,
    pub unique_moniker: String,
}

#[derive(Debug)]
pub struct Reader(pub ReaderId, pub ReaderInfo);

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookReadInfo {
    // This thing should not be needed here. Move into the event
    pub reader_id: ReaderId,
    pub book_id: BookId,
    pub when: Option<OffsetDateTime>,
}

impl AggregateRoot for Author {
    type Id = AuthorId;

    fn try_load(stream: AggregateStream) -> Result<Self> {
        if let Event::AuthorAdded(id, info) = stream.peek()? {
            Ok(Author(id, info))
        } else {
            Err(Error::AggregateParseError(
                "expected an AuthorAdded".to_owned(),
            ))
        }
    }
}

impl AggregateIdentity for AuthorId {
    type Root = Author;

    fn id(&self) -> &UniqueId {
        let AuthorId(id) = self;
        id
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookId(pub UniqueId);

impl From<BookId> for uuid::Uuid {
    fn from(BookId(UniqueId(id)): BookId) -> Self {
        id
    }
}

pub enum ResourceId {
    Book(BookId),
    Author(AuthorId),
    Reader(ReaderId),
}

impl From<BookId> for ResourceId {
    fn from(value: BookId) -> Self {
        Self::Book(value)
    }
}

impl From<AuthorId> for ResourceId {
    fn from(value: AuthorId) -> Self {
        Self::Author(value)
    }
}

impl From<ReaderId> for ResourceId {
    fn from(value: ReaderId) -> Self {
        Self::Reader(value)
    }
}

impl AggregateRoot for Book {
    type Id = BookId;

    fn try_load(stream: AggregateStream) -> Result<Self> {
        // This can be simplified
        if let Event::BookAdded(id, info) = stream.peek()? {
            Ok(Book(id, info))
        } else {
            Err(Error::AggregateParseError(
                "Expected a BookAdded".to_owned(),
            ))
        }
    }
}

impl AggregateIdentity for BookId {
    type Root = Book;

    fn id(&self) -> &UniqueId {
        let BookId(id) = self;
        id
    }
}
