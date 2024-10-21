use serde::{Deserialize, Serialize};
use std::time::SystemTime;

use crate::{
    error::{Error, Result},
    infrastructure::{
        AggregateIdentity, AggregateRoot, AggregateStream, EventDescriptor, ExternalRepresentation,
        UniqueId,
    },
};

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
            otherwise => Err(Error::UnknownEventType(otherwise.to_owned())),
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorId(pub UniqueId);

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

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookId(pub UniqueId);

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

pub mod query {
    use std::collections::HashMap;

    use crate::core::model::{Author, AuthorId, AuthorInfo, Book, BookId, BookInfo, Event};

    #[derive(Debug, Default)]
    pub struct IndexSet {
        authors: HashMap<AuthorId, AuthorInfo>,
        books: HashMap<BookId, BookInfo>,
        books_by_author_id: HashMap<AuthorId, Vec<BookId>>,
    }

    impl IndexSet {
        pub fn apply(&mut self, event: Event) {
            match event {
                Event::BookAdded(id, info) => {
                    self.books.insert(id.clone(), info.clone());
                    self.books_by_author_id
                        .entry(info.author)
                        .or_default()
                        .push(id);
                }
                Event::AuthorAdded(id, info) => {
                    self.authors.insert(id, info);
                }
            }
        }
    }

    pub trait IndexSetQuery {
        type Output;

        fn execute(&self, model: &IndexSet) -> Self::Output;
    }

    pub struct AllBooks;

    impl IndexSetQuery for AllBooks {
        type Output = Vec<Book>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            index
                .books
                .iter()
                .map(|(id, info)| Book(id.clone(), info.clone()))
                .collect()
        }
    }

    pub struct BookById(pub BookId);

    impl IndexSetQuery for BookById {
        type Output = Option<Book>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            let BookById(id) = self;
            index
                .books
                .get(id)
                .map(|info| Book(id.clone(), info.clone()))
        }
    }

    pub struct AllAuthors;

    impl IndexSetQuery for AllAuthors {
        type Output = Vec<Author>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            index
                .authors
                .iter()
                .map(|(id, info)| Author(id.clone(), info.clone()))
                .collect()
        }
    }

    pub struct BooksByAuthorId(pub AuthorId);

    impl IndexSetQuery for BooksByAuthorId {
        type Output = Vec<Book>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            let BooksByAuthorId(author_id) = self;
            if let Some(book_ids) = index.books_by_author_id.get(author_id) {
                book_ids
                    .iter()
                    .filter_map(|id| {
                        index
                            .books
                            .get(id)
                            .map(|info| Book(id.clone(), info.clone()))
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![]
            }
        }
    }
}
