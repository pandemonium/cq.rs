use anyhow::{Error as AnyhowError, Result};
use clap::{Parser, Subcommand};
use std::{collections::HashMap, fmt, str::FromStr};
use uuid::Uuid;

use super::domain;

#[derive(Subcommand)]
pub enum Command {
    AddAuthor(AuthorInfo),
    AddBook(BookInfo),
    AddReader(ReaderInfo),
    ReadBook(BookRead),
    ListAuthors,
    ListBooks,
    ListReaders,
    ListReadBooks {
        #[arg(long)]
        reader_ref: ReaderRef,
    },
}

#[derive(Parser)]
pub struct BookInfo {
    #[arg(long, help = "Title of the book")]
    pub title: String,

    #[arg(long, help = "ISBN of the book")]
    pub isbn: String,

    #[arg(long, help = "ID of the author")]
    pub author_id: String,
}

impl TryFrom<BookInfo> for domain::BookInfo {
    type Error = AnyhowError;

    fn try_from(
        BookInfo {
            title,
            isbn,
            author_id,
        }: BookInfo,
    ) -> Result<Self> {
        Ok(Self {
            isbn,
            title,
            author: domain::AuthorId(author_id.parse()?),
        })
    }
}

#[derive(Parser)]
pub struct AuthorInfo {
    #[arg(long, help = "Name of the author")]
    pub name: String,
}

impl TryFrom<AuthorInfo> for domain::AuthorInfo {
    type Error = AnyhowError;

    fn try_from(AuthorInfo { name }: AuthorInfo) -> Result<Self> {
        Ok(Self { name })
    }
}

#[derive(Clone)]
pub struct Author(domain::Author);

impl fmt::Display for Author {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(domain::Author {
            id: domain::AuthorId(id),
            info: domain::AuthorInfo { name },
        }) = self;
        write!(f, "[{id}]\t{name}")
    }
}

impl From<domain::Author> for Author {
    fn from(value: domain::Author) -> Self {
        Self(value)
    }
}

#[derive(Clone)]
pub struct Book(domain::Book);

impl fmt::Display for Book {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(domain::Book {
            id: domain::BookId(id),
            info: domain::BookInfo { title, .. },
        }) = self;

        // I would like the author name here.
        // Display this multilined (can this be configurable?)
        write!(f, "[{id}] {title}")
    }
}

impl From<domain::Book> for Book {
    fn from(value: domain::Book) -> Self {
        Self(value)
    }
}

#[derive(Parser)]
pub struct ReaderInfo {
    name: String,
    unique_moniker: String,
}

impl TryFrom<ReaderInfo> for domain::ReaderInfo {
    type Error = AnyhowError;

    fn try_from(
        ReaderInfo {
            name,
            unique_moniker,
        }: ReaderInfo,
    ) -> Result<Self> {
        Ok(Self {
            name,
            unique_moniker,
        })
    }
}

pub struct Reader(domain::Reader);

impl From<domain::Reader> for Reader {
    fn from(value: domain::Reader) -> Self {
        Self(value)
    }
}

impl fmt::Display for Reader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(domain::Reader {
            id: domain::ReaderId(_0),
            info:
                domain::ReaderInfo {
                    name,
                    unique_moniker,
                },
        }) = self;
        write!(f, "{name} [{unique_moniker}]")
    }
}

pub struct BookWithAuthor(Book, Author);

impl BookWithAuthor {
    pub fn joined(books: Vec<domain::Book>, authors: Vec<domain::Author>) -> Vec<BookWithAuthor> {
        let author_by_id = authors
            .iter()
            .map(|x| (&x.id, x))
            .collect::<HashMap<_, _>>();

        books
            .into_iter()
            .filter_map(|book| {
                author_by_id
                    .get(&book.info.author)
                    .map(|&author| Self(book.into(), author.clone().into()))
            })
            .collect()
    }
}

impl fmt::Display for BookWithAuthor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(
            Book(domain::Book {
                id: domain::BookId(book_id),
                info: domain::BookInfo { isbn, title, .. },
            }),
            Author(domain::Author {
                id: domain::AuthorId(author_id),
                info: domain::AuthorInfo { name: author_name },
            }),
        ) = self;
        writeln!(f, "{title} [{isbn}]")?;
        writeln!(f, "{author_name} [Author ID {author_id}]")?;
        write!(f, "[Book ID {book_id}]")
    }
}

#[derive(Parser)]
pub struct BookRead {
    pub reader_moniker: String,
    pub book_id: String,
}

#[derive(Clone)]
pub enum ReaderRef {
    ByReaderId(Uuid),
    ByUniqueMoniker(String),
}

impl FromStr for ReaderRef {
    type Err = AnyhowError;

    fn from_str(s: &str) -> Result<Self> {
        if let Ok(reader_id) = Uuid::parse_str(s) {
            Ok(ReaderRef::ByReaderId(reader_id))
        } else {
            Ok(ReaderRef::ByUniqueMoniker(s.to_owned()))
        }
    }
}
