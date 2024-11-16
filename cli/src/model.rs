use anyhow::{Error as AnyhowError, Result};
use clap::{Parser, Subcommand, ValueEnum};
use std::{collections::HashMap, fmt, str::FromStr};
use tabled::{builder::Builder, settings::Style};
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
    Search {
        #[arg(value_name = "search-term", help = "Term to search for")]
        search_term: String,
    },
    Import(ImportSpec),
}

#[derive(Parser)]
pub struct ImportSpec {
    #[arg(long, value_enum)]
    pub format: ImportFormat,

    pub from: String,
}

#[derive(Clone, ValueEnum)]
pub enum ImportFormat {
    Csv,
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

impl Author {
    pub fn table(data: Vec<Self>) -> String {
        let mut builder = Builder::default();
        builder.push_record(vec!["", "Id", "Name"]);

        for (index, Author(author)) in data.into_iter().enumerate() {
            builder.push_record(vec![
                format!("{}", index + 1),
                author.id.to_string(),
                author.info.name,
            ])
        }

        let sharp = Style::sharp();
        builder.build().with(sharp).to_string()
    }
}

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
            info:
                domain::ReaderInfo {
                    name,
                    unique_moniker,
                },
            ..
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

    pub fn table(data: Vec<Self>) -> String {
        let mut builder = Builder::default();
        builder.push_record(vec!["", "Id", "Title", "Author", "ISBN"]);

        for (index, BookWithAuthor(Book(book), Author(author))) in data.into_iter().enumerate() {
            builder.push_record(vec![
                format!("{}", index + 1),
                book.id.to_string(),
                book.info.title,
                author.info.name,
                book.info.isbn,
            ])
        }

        builder.build().with(Style::sharp()).to_string()
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

pub struct SearchResultItem(domain::SearchResultItem);

impl SearchResultItem {
    pub fn table(data: Vec<Self>) -> String {
        let mut builder = Builder::default();
        builder.push_record(vec!["", "Kind", "Hit", "ID"]);

        for (index, SearchResultItem(domain::SearchResultItem { hit, .. })) in
            data.into_iter().enumerate()
        {
            let mut fields = vec![format!("{}", index + 1)];

            // Should this re-constitute the underlying resource?
            match hit {
                domain::SearchHit::BookTitle { title, id } => {
                    fields.extend(vec!["Book".to_owned(), title, id.to_string()]);
                }
                domain::SearchHit::BookIsbn { isbn, id } => {
                    fields.extend(vec!["Book".to_owned(), isbn, id.to_string()]);
                }
                domain::SearchHit::Author { name, id } => {
                    fields.extend(vec!["Author".to_owned(), name, id.to_string()]);
                }
            }

            builder.push_record(fields);
        }

        builder.build().with(Style::sharp()).to_string()
    }
}

impl From<domain::SearchResultItem> for SearchResultItem {
    fn from(value: domain::SearchResultItem) -> Self {
        Self(value)
    }
}
