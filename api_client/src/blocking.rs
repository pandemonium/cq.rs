use reqwest::blocking::Client;
use serde::{de::DeserializeOwned, Serialize};

use crate::{error, model};

#[derive(Clone)]
pub struct ApiClient {
    http_client: Client,
    base_url: String,
}

impl ApiClient {
    const API_RESOURCE_PREFIX: &str = "/api/v1";

    pub fn new(base_url: &str) -> Self {
        let http_client = Client::new();
        // See to it that base_url does not end in /
        Self {
            http_client,
            base_url: base_url.to_owned(),
        }
    }

    pub fn get_books(&self) -> error::Result<Vec<model::Book>> {
        self.request_resource("/books")
    }

    pub fn get_authors(&self) -> error::Result<Vec<model::Author>> {
        self.request_resource("/authors")
    }

    pub fn get_readers(&self) -> error::Result<Vec<model::Reader>> {
        self.request_resource("/readers")
    }

    pub fn get_author_by_book(
        &self,
        book_id: model::BookId,
    ) -> error::Result<Option<model::Author>> {
        self.request_resource(&format!("/books/{book_id}/author"))
    }

    pub fn get_books_by_author(
        &self,
        author_id: model::AuthorId,
    ) -> error::Result<Vec<model::Book>> {
        self.request_resource(&format!("/authors/{author_id}/books"))
    }

    pub fn get_books_read(&self, reader_id: model::ReaderId) -> error::Result<Vec<model::Book>> {
        self.request_resource(&format!("/readers/{reader_id}/books"))
    }

    pub fn add_author(&self, info: model::AuthorInfo) -> error::Result<()> {
        self.post_resource("/authors", info)
    }

    pub fn add_book(&self, info: model::BookInfo) -> error::Result<()> {
        self.post_resource("/books", info)
    }

    pub fn add_reader(&self, info: model::ReaderInfo) -> error::Result<()> {
        self.post_resource("/readers", info)
    }

    pub fn get_reader_by_moniker(&self, moniker: &str) -> error::Result<Option<model::Reader>> {
        self.request_resource(&format!("/readers/moniker/{}", moniker))
    }

    pub fn add_read_book(&self, info: model::BookRead) -> error::Result<()> {
        self.post_resource(&format!("/books/{}/readers", &info.book_id), info)
    }

    pub fn search(&self, query_text: &str) -> error::Result<Vec<model::SearchResultItem>> {
        let resource_uri = self.resolve_resource_uri("/search");
        let request = self
            .http_client
            .get(resource_uri)
            .query(&[("query", query_text)])
            .build()?;
        let response = self.http_client.execute(request)?;
        Ok(serde_json::from_slice(&response.bytes()?)?)
    }

    fn post_resource<R>(&self, uri: &str, resource: R) -> error::Result<()>
    where
        R: Serialize,
    {
        let resource_uri = self.resolve_resource_uri(uri);
        let request = self
            .http_client
            .post(resource_uri)
            .json(&resource)
            .build()?;
        let response = self.http_client.execute(request)?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(error::Error::Server(response.status()))
        }
    }

    // An ADT can be constructed around the Resource abstraction to deal
    // with the ugly stringly typed mess of paths that it is currently
    fn request_resource<R>(&self, resource_uri: &str) -> error::Result<R>
    where
        R: DeserializeOwned,
    {
        let resource_uri = self.resolve_resource_uri(resource_uri);
        let request = self.http_client.get(resource_uri).build()?;
        let response = self.http_client.execute(request)?;
        Ok(serde_json::from_slice(&response.bytes()?)?)
    }

    fn resolve_resource_uri(&self, resource_uri: &str) -> String {
        format!(
            "{}{}{resource_uri}",
            self.base_url,
            Self::API_RESOURCE_PREFIX
        )
    }
}