use anyhow::{anyhow, Result};
use core::fmt;
use csv::ReaderBuilder;
use isbn;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufRead, BufReader},
    path::PathBuf,
    str::FromStr,
};
use uuid::Uuid;

use api_client::{model as domain, ApiClient};

pub async fn from_source(api: ApiClient, source: ImportSource) -> Result<()> {
    let csv_data = read_csv_data(source.make_reader()?);
    Importer { api }
        .compute_import_delta(&csv_data?)
        .await?
        .import()
        .await
}

pub enum ImportSource {
    StdIn,
    FilePath(PathBuf),
}

impl ImportSource {
    fn make_reader(&self) -> Result<Box<dyn BufRead>> {
        Ok(match self {
            ImportSource::StdIn => Box::new(BufReader::new(io::stdin())),
            ImportSource::FilePath(path) => Box::new(BufReader::new(File::open(path)?)),
        })
    }
}

impl FromStr for ImportSource {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(if s == "-" {
            ImportSource::StdIn
        } else {
            ImportSource::FilePath(PathBuf::from_str(s)?)
        })
    }
}

struct Importer {
    api: ApiClient,
}

impl Importer {
    // I would like to be able to present this in a --dry-run setting
    // so that it can be inspected
    async fn compute_import_delta(self, data: &[DataRow]) -> Result<ImportDelta> {
        // A little ugly that this owns the API client
        let mut import = ImportDelta::new(self.api);

        for DataRow {
            title,
            isbn,
            author,
        } in data
        {
            let isbn: Isbn = isbn.parse()?;

            // ... and that these calls happen through the commit.
            if import.find_existing_book(title, &isbn).await?.is_none() {
                let author_id = import.get_canonical_author_ref(author).await?;
                import.add_book(NewBook {
                    title: title.to_owned(),
                    isbn,
                    author_id,
                });
            }
        }

        Ok(import)
    }
}

enum AuthorId {
    New(Uuid),
    Existing(domain::AuthorId),
}

struct ImportDelta {
    api: ApiClient,
    new_authors: HashMap<Uuid, String>,
    books: Vec<NewBook>,
}

impl ImportDelta {
    fn new(api: ApiClient) -> Self {
        Self {
            api,
            new_authors: Default::default(),
            books: Default::default(),
        }
    }

    async fn get_canonical_author_ref(&mut self, author_name: &str) -> Result<AuthorId> {
        if let Some(author_id) = self.find_existing_author(author_name).await? {
            Ok(AuthorId::Existing(author_id.clone()))
        } else {
            let id = Uuid::new_v4();
            self.new_authors.insert(id, author_name.to_owned());
            Ok(AuthorId::New(id))
        }
    }

    async fn find_existing_author(&self, author_name: &str) -> Result<Option<domain::AuthorId>> {
        Ok(self.api.search(author_name).await?.into_iter().find_map(
            |domain::SearchResultItem { hit, .. }| match hit {
                domain::SearchHit::Author { name, id } if name == author_name => Some(id),
                _otherwise => None,
            },
        ))
    }

    async fn find_existing_book(
        &self,
        book_title: &str,
        book_isbn: &Isbn,
    ) -> Result<Option<domain::BookId>> {
        let book_isbn = book_isbn.to_string();
        let hits = self.api.search(&book_isbn).await?;

        let xs: HashSet<domain::BookId> = hits
            .into_iter()
            .filter_map(|domain::SearchResultItem { hit, .. }| match hit {
                domain::SearchHit::BookTitle { title, id } if title == book_title => Some(id),
                domain::SearchHit::BookIsbn { isbn, id } if isbn == book_isbn => Some(id),
                _otherwise => None,
            })
            .collect();

        Ok(xs.into_iter().next())
    }

    fn add_book(&mut self, book: NewBook) {
        self.books.push(book);
    }

    async fn import(self) -> Result<()> {
        let mut authors = HashMap::new();

        for (id, name) in self.new_authors {
            authors.insert(id, self.api.add_author(domain::AuthorInfo { name }).await?);
        }

        for NewBook {
            title,
            isbn,
            author_id,
        } in self.books
        {
            let author = match author_id {
                AuthorId::New(uuid) => authors
                    .get(&uuid)
                    .expect("author should have been created")
                    .clone(),
                AuthorId::Existing(author_id) => author_id,
            };

            self.api
                .add_book(domain::BookInfo {
                    isbn: isbn.to_string(),
                    title,
                    author,
                })
                .await?;
        }

        Ok(())
    }
}

struct Isbn(isbn::Isbn);

impl FromStr for Isbn {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(Self(s.parse().map_err(|e| anyhow!("{e}"))?))
    }
}

impl fmt::Display for Isbn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(inner) = self;
        Ok(write!(f, "{}", inner.hyphenate().expect("invalid ISBN"))?)
    }
}

struct NewBook {
    title: String,
    isbn: Isbn,
    author_id: AuthorId,
}

#[derive(Deserialize)]
pub struct DataRow {
    title: String,
    isbn: String,
    author: String,
}

fn read_csv_data<R>(reader: R) -> Result<Vec<DataRow>>
where
    R: BufRead,
{
    let mut data = vec![];
    let mut csv = ReaderBuilder::new()
        .delimiter(b';')
        .has_headers(false)
        .from_reader(reader);
    for row in csv.deserialize() {
        data.push(row?);
    }
    Ok(data)
}
