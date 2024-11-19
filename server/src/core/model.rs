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
                data: serde_json::to_value(keyword)?,
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
            otherwise => Err(Error::UnknownEventType(otherwise.to_owned())),
        }
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
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

#[derive(Clone, Hash, PartialEq, Eq)]
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

static KEYWORD_REGEX: OnceLock<Regex> = OnceLock::new();

fn keyword_regex() -> &'static Regex {
    KEYWORD_REGEX.get_or_init(|| Regex::new(r"[\p{L}-_]+").expect("KEYWORD_REGEX is valid"))
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

pub mod query {
    use std::collections::{HashMap, HashSet};

    use crate::core::model::{
        Author, AuthorId, AuthorInfo, Book, BookId, BookInfo, BookReadInfo, Event, Reader,
        ReaderId, ReaderInfo,
    };

    #[derive(Debug, Default)]
    pub struct IndexSet {
        authors: HashMap<AuthorId, AuthorInfo>,
        books: HashMap<BookId, BookInfo>,
        readers: HashMap<ReaderId, ReaderInfo>,
        reader_by_moniker: HashMap<String, ReaderId>,
        books_by_reader_id: HashMap<ReaderId, HashSet<BookReadInfo>>,
        books_by_author_id: HashMap<AuthorId, Vec<BookId>>,
        texts: text::SearchIndex,
        keywords: keywords::Index,
    }

    impl IndexSet {
        pub fn apply(&mut self, event: Event) {
            self.texts.apply(&event);
            self.apply_event(event)
        }

        fn apply_event(&mut self, event: Event) {
            match event {
                Event::BookAdded(id, info) => {
                    self.books.insert(id, info.clone());
                    self.books_by_author_id
                        .entry(info.author)
                        .or_default()
                        .push(id);
                }
                Event::AuthorAdded(id, info) => {
                    self.authors.insert(id, info);
                }
                Event::ReaderAdded(id, info) => {
                    self.reader_by_moniker
                        .insert(info.unique_moniker.clone(), id);
                    self.readers.insert(id, info);
                }
                Event::BookRead(id, info) => {
                    self.books_by_reader_id.entry(id).or_default().insert(info);
                }
                Event::KeywordAdded(target, keyword) => {
                    self.keywords.add_keyword_to_target(keyword, target)
                }
            }
        }
    }

    pub trait IndexSetQuery {
        type Output;

        fn execute(&self, index: &IndexSet) -> Self::Output;
    }

    pub struct AllBooks;

    impl IndexSetQuery for AllBooks {
        type Output = Vec<Book>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            index
                .books
                .iter()
                .map(|(id, info)| Book(*id, info.clone()))
                .collect()
        }
    }

    pub struct BookById(pub BookId);

    impl IndexSetQuery for BookById {
        type Output = Option<Book>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            let Self(id) = self;
            index.books.get(id).map(|info| Book(*id, info.clone()))
        }
    }

    pub struct AuthorById(pub AuthorId);

    impl IndexSetQuery for AuthorById {
        type Output = Option<Author>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            let Self(id) = self;
            index.authors.get(id).map(|info| Author(*id, info.clone()))
        }
    }

    pub struct AuthorByBookId(pub BookId);

    impl IndexSetQuery for AuthorByBookId {
        type Output = Option<Author>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            let Self(id) = self;
            index.books.get(id).and_then(|BookInfo { author, .. }| {
                index
                    .authors
                    .get(author)
                    .map(|info| Author(*author, info.clone())) // this pattern repeats.
            })
        }
    }

    pub struct BooksByReader(pub ReaderId);

    impl IndexSetQuery for BooksByReader {
        type Output = Vec<Book>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            let Self(id) = self;

            index
                .books_by_reader_id
                .get(id)
                .and_then(|read_books| {
                    read_books
                        .iter()
                        .map(|BookReadInfo { book_id, .. }| {
                            index
                                .books
                                .get(book_id)
                                .map(|info| Book(*book_id, info.clone()))
                        })
                        .collect::<Option<Vec<_>>>()
                })
                .unwrap_or_default()
        }
    }

    pub struct AllAuthors;

    impl IndexSetQuery for AllAuthors {
        type Output = Vec<Author>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            index
                .authors
                .iter()
                .map(|(id, info)| Author(*id, info.clone()))
                .collect()
        }
    }

    pub struct BooksByAuthorId(pub AuthorId);

    impl IndexSetQuery for BooksByAuthorId {
        type Output = Vec<Book>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            let Self(author_id) = self;
            if let Some(book_ids) = index.books_by_author_id.get(author_id) {
                book_ids
                    .iter()
                    .filter_map(|id| index.books.get(id).map(|info| Book(*id, info.clone())))
                    .collect::<Vec<_>>()
            } else {
                vec![]
            }
        }
    }

    pub struct AllReaders;

    impl IndexSetQuery for AllReaders {
        type Output = Vec<Reader>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            index
                .readers
                .iter()
                .map(|(id, info)| Reader(*id, info.clone()))
                .collect()
        }
    }

    pub struct ReaderById(pub ReaderId);

    impl IndexSetQuery for ReaderById {
        type Output = Option<Reader>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            let Self(id) = self;
            index.readers.get(id).map(|info| Reader(*id, info.clone()))
        }
    }

    pub struct UniqueReaderByMoniker(pub String);

    impl IndexSetQuery for UniqueReaderByMoniker {
        type Output = Option<Reader>;

        fn execute(&self, index: &IndexSet) -> Self::Output {
            let Self(moniker) = self;
            index
                .reader_by_moniker
                .get(moniker.as_str())
                .and_then(|reader_id| {
                    index
                        .readers
                        .get(reader_id)
                        .map(|info| Reader(*reader_id, info.clone()))
                })
        }
    }

    pub mod keywords {
        use bimap::BiHashMap;

        use super::*;
        use crate::core::model::KeywordTarget;

        #[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
        struct KeywordId(u16);

        #[derive(Debug, Default)]
        struct KeywordMap {
            next_id: u16,
            inner: BiHashMap<String, KeywordId>,
        }

        impl KeywordMap {
            fn get_or_reserve_id(&mut self, keyword: String) -> KeywordId {
                self.inner
                    .get_by_left(&keyword)
                    .copied()
                    .unwrap_or_else(|| {
                        let id = KeywordId(self.next_id);
                        self.inner.insert(keyword, id);
                        self.next_id += 1;
                        id
                    })
            }

            fn lookup_id(&self, keyword: &str) -> Option<&KeywordId> {
                self.inner.get_by_left(keyword)
            }

            fn resolve(&self, id: KeywordId) -> Option<&str> {
                self.inner.get_by_right(&id).map(|s| s.as_str())
            }
        }

        #[derive(Debug, Default)]
        pub struct Index {
            keyword_map: KeywordMap,
            target_keywords: HashMap<KeywordTarget, HashSet<KeywordId>>,
            keyword_targets: HashMap<KeywordId, HashSet<KeywordTarget>>,
        }

        impl Index {
            pub fn add_keyword_to_target(&mut self, keyword: String, target: KeywordTarget) {
                let id = self.keyword_map.get_or_reserve_id(keyword);
                self.target_keywords.entry(target).or_default().insert(id);
                self.keyword_targets.entry(id).or_default().insert(target);
            }

            pub fn get_keywords(&self, target: &KeywordTarget) -> Vec<String> {
                self.target_keywords
                    .get(target)
                    .map(|keywords| {
                        keywords
                            .iter()
                            .map(|id| {
                                self.keyword_map
                                    .resolve(*id)
                                    .map(|s| s.to_owned())
                                    .expect("consistent maps")
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            }

            pub fn remove_keyword_from_target(&mut self, keyword: &str, target: KeywordTarget) {
                if let Some(id) = self.keyword_map.lookup_id(keyword) {
                    if let Some(targets) = self.keyword_targets.get_mut(id) {
                        targets.remove(&target);
                        self.target_keywords.remove(&target);
                    }
                }
            }
        }
    }

    pub mod text {
        use std::{
            cmp::Eq,
            collections::{HashMap, HashSet},
        };

        use crate::core::model::{
            query::{IndexSet, IndexSetQuery},
            AuthorId, AuthorInfo, BookId, BookInfo, Event, Isbn,
        };

        const SEARCH_TERM_LENGTH_THRESHOLD: usize = 1;

        fn tokenize(phrase: &str) -> Vec<&str> {
            phrase
                .split(&[' ', ',', '.', '-', '(', ')'])
                .filter(|term| term.len() > SEARCH_TERM_LENGTH_THRESHOLD)
                .collect()
        }

        // Move to super-module - this must not be publically
        // accessible from the http module
        #[derive(Debug, Default)]
        pub struct SearchIndex {
            term_projections: HashMap<String, HashSet<Projection>>,
        }

        impl SearchIndex {
            pub fn apply(&mut self, event: &Event) {
                match event {
                    Event::BookAdded(
                        id,
                        BookInfo {
                            isbn: Isbn(isbn),
                            title,
                            ..
                        },
                    ) => {
                        let this_book = Projection::Books(BookField::Isbn(*id));
                        self.bind_term(isbn, this_book);
                        self.index_phrase(title, Projection::Books(BookField::Title(*id)));
                    }
                    Event::AuthorAdded(id, AuthorInfo { name }) => {
                        self.index_phrase(name, Projection::Authors(AuthorField::Name(*id)));
                    }
                    // Don't index these
                    Event::ReaderAdded(..) => (),
                    Event::BookRead(..) => (),
                    // Think about this.
                    Event::KeywordAdded(..) => (),
                }
            }

            fn index_phrase(&mut self, phrase: &str, target: Projection) {
                for token in tokenize(phrase) {
                    self.bind_term(token, target)
                }
            }

            fn bind_term(&mut self, term: &str, target: Projection) {
                self.term_projections
                    .entry(term.to_owned())
                    .or_default()
                    .insert(target);
            }

            pub fn lookup(&self, term: &str) -> Vec<Projection> {
                if let Some(xs) = self.term_projections.get(term) {
                    xs.iter().copied().collect()
                } else {
                    vec![]
                }
            }
        }

        // SearchQuery with multiple terms that return intersection(hits*)
        pub struct SearchQuery(pub String);

        pub struct SearchHit {
            pub target: Projection,
            pub source: String,
        }

        impl IndexSetQuery for SearchQuery {
            type Output = Vec<SearchHit>;

            fn execute(&self, index: &IndexSet) -> Self::Output {
                let mut hits = vec![];

                let SearchQuery(search_term) = self;
                for projection in index.texts.lookup(search_term) {
                    if let Some(hit) = resolve_projection(projection, index) {
                        hits.push(hit)
                    } else {
                        panic!("Text index has data that is not reflected in the field indices.")
                    }
                }

                hits
            }
        }

        // It would look good to have this on IndexSet, but ... what?
        fn resolve_projection(target: Projection, index: &IndexSet) -> Option<SearchHit> {
            let source = match &target {
                Projection::Books(BookField::Isbn(id)) => index.books.get(id).map(
                    |BookInfo {
                         isbn: Isbn(isbn), ..
                     }| isbn,
                ),
                Projection::Books(BookField::Title(id)) => index.books.get(id).map(|x| &x.title),
                Projection::Authors(AuthorField::Name(id)) => {
                    index.authors.get(id).map(|x| &x.name)
                }
            };

            source.map(|source| SearchHit {
                target,
                source: source.to_owned(),
            })
        }

        #[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
        pub enum Projection {
            Books(BookField),
            Authors(AuthorField),
        }

        #[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
        pub enum BookField {
            Title(BookId),
            Isbn(BookId),
        }

        #[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
        pub enum AuthorField {
            Name(AuthorId),
        }
    }
}
