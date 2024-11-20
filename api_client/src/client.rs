use reqwest::Client;
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

    pub async fn get_books(&self) -> error::Result<Vec<model::Book>> {
        self.request_resource("/books").await
    }

    pub async fn get_authors(&self) -> error::Result<Vec<model::Author>> {
        self.request_resource("/authors").await
    }

    pub async fn get_readers(&self) -> error::Result<Vec<model::Reader>> {
        self.request_resource("/readers").await
    }

    pub async fn get_book_keywords(&self, id: model::BookId) -> error::Result<Vec<String>> {
        self.request_resource(&format!("/books/{}/keywords", id))
            .await
    }

    pub async fn get_author_keywords(&self, id: model::AuthorId) -> error::Result<Vec<String>> {
        self.request_resource(&format!("/authors/{}/keywords", id))
            .await
    }

    pub async fn get_author_by_book(
        &self,
        book_id: model::BookId,
    ) -> error::Result<Option<model::Author>> {
        self.request_resource(&format!("/books/{book_id}/author"))
            .await
    }

    pub async fn get_books_by_author(
        &self,
        author_id: model::AuthorId,
    ) -> error::Result<Vec<model::Book>> {
        self.request_resource(&format!("/authors/{author_id}/books"))
            .await
    }

    pub async fn get_books_read(
        &self,
        reader_id: model::ReaderId,
    ) -> error::Result<Vec<model::Book>> {
        self.request_resource(&format!("/readers/{reader_id}/books"))
            .await
    }

    pub async fn get_keyword_targets(
        &self,
        keyword: String,
    ) -> error::Result<model::KeywordTarget> {
        self.request_resource(&format!("/keywords/{keyword}/targets"))
            .await
    }

    pub async fn add_author(&self, info: model::AuthorInfo) -> error::Result<model::AuthorId> {
        let resource_id: model::ResourceId = self.post_resource("/authors", info).await?;
        Ok(model::AuthorId(resource_id.id))
    }

    pub async fn add_book(&self, info: model::BookInfo) -> error::Result<model::BookId> {
        let resource_id: model::ResourceId = self.post_resource("/books", info).await?;
        Ok(model::BookId(resource_id.id))
    }

    pub async fn add_reader(&self, info: model::ReaderInfo) -> error::Result<model::ReaderId> {
        let resource_id: model::ResourceId = self.post_resource("/readers", info).await?;
        Ok(model::ReaderId(resource_id.id))
    }

    pub async fn add_keyword_to_book(
        &self,
        id: model::BookId,
        keyword: String,
    ) -> error::Result<()> {
        Ok(self
            .post_resource(&format!("/books/{id}/keywords"), keyword)
            .await?)
    }

    pub async fn add_keyword_to_author(
        &self,
        id: model::AuthorId,
        keyword: String,
    ) -> error::Result<()> {
        Ok(self
            .post_resource(&format!("/authors/{id}/keywords"), keyword)
            .await?)
    }

    pub async fn get_reader_by_moniker(
        &self,
        moniker: &str,
    ) -> error::Result<Option<model::Reader>> {
        self.request_resource(&format!("/readers/moniker/{}", moniker))
            .await
    }

    pub async fn add_read_book(&self, info: model::BookRead) -> error::Result<()> {
        self.post_resource(&format!("/books/{}/readers", &info.book_id), info)
            .await
    }

    pub async fn search(&self, query_text: &str) -> error::Result<Vec<model::SearchResultItem>> {
        let resource_uri = self.resolve_resource_uri("/search");
        let request = self
            .http_client
            .get(resource_uri)
            .query(&[("query", query_text)])
            .build()?;
        let response = self.http_client.execute(request).await?;
        Ok(serde_json::from_slice(&response.bytes().await?)?)
    }

    async fn post_resource<R, S>(&self, uri: &str, resource: R) -> error::Result<S>
    where
        R: Serialize,
        S: DeserializeOwned,
    {
        let resource_uri = self.resolve_resource_uri(uri);
        let request = self
            .http_client
            .post(resource_uri)
            .json(&resource)
            .build()?;
        let response = self.http_client.execute(request).await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            Err(error::Error::Server(response.status()))
        }
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

    fn resolve_resource_uri(&self, resource_uri: &str) -> String {
        format!(
            "{}{}{resource_uri}",
            self.base_url,
            Self::API_RESOURCE_PREFIX
        )
    }
}
