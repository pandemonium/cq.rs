use crate::infrastructure::{
    AggregateIdentity, AggregateRoot, AggregateStream, EventDescriptor, ExternalRepresentation,
    UniqueId,
};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Clone, Debug)]
pub enum Event {
    BookAdded(BookId, BookInfo),
    AuthorAdded(AuthorId, AuthorInfo),
}

impl Event {
    const BOOK_ADDED: &str = "book-added";
    const AUTHOR_ADDED: &str = "author-added";

    fn name(&self) -> &str {
        match self {
            Event::BookAdded(..) => Self::BOOK_ADDED,
            Event::AuthorAdded(..) => Self::AUTHOR_ADDED,
        }
    }
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
            otherwise => Err(anyhow!("Unknown event-type `{}`", otherwise)),
        }
    }
}

#[derive(Debug)]
pub struct Author(pub AuthorId, pub AuthorInfo);

#[derive(Debug)]
pub struct Book(pub BookId, pub BookInfo);

pub enum Command {
    AddBook(BookInfo),
    AddAuthor(AuthorInfo),
}

//pub struct AuthorById(AuthorId);

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

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorId(pub UniqueId);

impl AggregateRoot for Author {
    type Id = AuthorId;

    fn try_load(stream: AggregateStream) -> Result<Self> {
        if let Event::AuthorAdded(id, info) = stream.peek()? {
            Ok(Author(id, info))
        } else {
            Err(anyhow!("expected an AuthorAdded"))
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookId(pub UniqueId);

impl AggregateRoot for Book {
    type Id = BookId;

    fn try_load(stream: AggregateStream) -> Result<Self> {
        if let Event::BookAdded(id, info) = stream.peek()? {
            Ok(Book(id, info))
        } else {
            Err(anyhow!("expected an BookAdded"))
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
