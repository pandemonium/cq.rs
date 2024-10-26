use model::Book;
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize};

struct ApiClient {
    http_client: Client,
    base_url: String,
}

impl ApiClient {
    const API_RESOURCE_PREFIX: &str = "/api/v1";

    fn new(base_url: &str) -> Self {
        let http_client = Client::new();
        // See to it that base_url does not end in /
        Self {
            http_client,
            base_url: base_url.to_owned(),
        }
    }

    fn resolve_resource_uri(&self, resource_uri: &str) -> String {
        format!(
            "{}{}{resource_uri}",
            self.base_url,
            Self::API_RESOURCE_PREFIX
        )
    }

    pub async fn get_books(&self) -> error::Result<Vec<model::Book>> {
        self.request_resource("/books").await
    }

    pub async fn get_authors(&self) -> error::Result<Vec<model::Author>> {
        self.request_resource("/authors").await
    }

    pub async fn get_author_by_book(
        &self,
        book_id: model::BookId,
    ) -> error::Result<Option<model::Author>> {
        self.request_resource(&format!("/books/{}/author", book_id))
            .await
    }

    pub async fn get_books_by_author(
        &self,
        author_id: model::AuthorId,
    ) -> error::Result<Vec<model::Book>> {
        self.request_resource(&format!("/authors/{}/books", author_id))
            .await
    }

    // An ADT can be constructed around the Resource abstraction to deal
    // with the ugly stringly typed mess of paths that it is currently
    async fn request_resource<R>(&self, resource_uri: &str) -> error::Result<R>
    where
        R: DeserializeOwned,
    {
        let resource_uri = self.resolve_resource_uri(resource_uri);
        let request = self.http_client.get(resource_uri).build()?;
        let response = self.http_client.execute(request).await?;
        Ok(serde_json::from_slice(&response.bytes().await?)?)
    }
}

mod error {
    use std::result::Result as StdResult;

    use thiserror::Error;
    #[derive(Error, Debug)]
    pub enum Error {
        #[error("JSON marshalling failed {0}")]
        Json(#[from] serde_json::Error),

        #[error("HTTP IO failed {0}")]
        Http(#[from] reqwest::Error),
    }

    pub type Result<A> = StdResult<A, Error>;
}

// Think about extracting his and the stuff in http::model
// into a common crate "model types" or somesuch
mod model {
    use core::fmt;

    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Author {
        pub id: AuthorId,
        pub info: AuthorInfo,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct AuthorId(pub Uuid);

    #[derive(Debug, Serialize, Deserialize)]
    pub struct AuthorInfo {
        pub name: String,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Book {
        pub id: BookId,
        pub info: BookInfo,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct BookId(pub Uuid);

    #[derive(Debug, Serialize, Deserialize)]
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
}

#[tokio::main]
async fn main() {
    let client = ApiClient::new("http://dsky.local:3000");

    let books = client.get_books().await.expect("some books");
    println!("Books: {books:?}");

    let authors = client.get_authors().await.expect("some authors");
    println!("Authors: {authors:?}");

    for Book { id, info } in books {
        let author = client.get_author_by_book(id).await.expect("book's author");
        println!(
            "{} by {}",
            info.title,
            author.expect("existing author").info.name
        );
    }

    for model::Author { id, info } in authors {
        let books = client.get_books_by_author(id).await.expect("some books");
        println!("Books: {books:?}");
    }
}
