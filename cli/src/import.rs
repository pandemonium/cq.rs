use anyhow::Result;
use csv::ReaderBuilder;
use serde::Deserialize;
use std::{
    collections::HashMap,
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
        .compute_commit_delta(&csv_data?)
        .await?
        .send()
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
    async fn compute_commit_delta(self, data: &[DataRow]) -> Result<CommitDelta> {
        let mut commit = CommitDelta::new(self.api);

        for DataRow {
            title,
            isbn,
            author,
        } in data
        {
            if commit.find_existing_book(title, isbn).await?.is_none() {
                let author = commit.make_canonical_author(&author).await?;
                commit.add_book(NewBook {
                    title: title.to_owned(),
                    isbn: isbn.to_owned(),
                    author,
                });
            }
        }

        Ok(commit)
    }
}

struct Author {
    id: AuthorId,
    proxy: AuthorProxy,
}

enum AuthorId {
    Reference(domain::AuthorId),
    New(Uuid),
}

enum AuthorProxy {
    New(String),
    Reference(domain::AuthorId),
}

struct CommitDelta {
    api: ApiClient,
    new_authors: HashMap<Uuid, AuthorProxy>,
    books: Vec<NewBook>,
}

impl CommitDelta {
    fn new(api: ApiClient) -> Self {
        Self {
            api,
            new_authors: Default::default(),
            books: Default::default(),
        }
    }

    async fn make_canonical_author(&mut self, name: &str) -> Result<Author> {
        todo!()
    }

    async fn find_existing_book(&self, title: &str, isbn: &str) -> Result<Option<domain::BookId>> {
        todo!()
    }

    fn add_book(&mut self, book: NewBook) {
        self.books.push(book);
    }

    async fn send(self) -> Result<()> {
        let mut existing_authors: HashMap<Uuid, domain::AuthorId> = HashMap::default();

        for (id, proxy) in self.new_authors {
            match proxy {
                AuthorProxy::New(name) => {
                    self.api.add_author(domain::AuthorInfo { name }).await?;
                }
                AuthorProxy::Reference(author_id) => {
                    let _ = existing_authors.insert(id, author_id);
                }
            }
        }

        todo!()
    }
}

struct NewBook {
    title: String,
    isbn: String,
    author: Author,
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
    let mut csv = ReaderBuilder::new().from_reader(reader);
    for row in csv.deserialize() {
        data.push(row?);
    }
    Ok(data)
}
