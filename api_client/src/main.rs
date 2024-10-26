use reqwest::Client;

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

    fn resolve_uri(&self, resource_uri: &str) -> String {
        format!(
            "{}{}{resource_uri}",
            self.base_url,
            Self::API_RESOURCE_PREFIX
        )
    }

    pub async fn get_books(&self) -> error::Result<Vec<model::Book>> {
        let resource_uri = self.resolve_uri("/books");
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

mod model {
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Serialize, Deserialize)]
    pub struct Author;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct AuthorId(pub Uuid);

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Book {
        id: BookId,
        info: BookInfo,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct BookId(pub Uuid);

    #[derive(Debug, Serialize, Deserialize)]
    pub struct BookInfo {
        isbn: String,
        title: String,
        author: AuthorId,
    }
}

#[tokio::main]
async fn main() {
    let client = ApiClient::new("http://dsky.local:3000");
    let books = client.get_books().await.expect("some books");

    println!("Books: {books:?}");
}
