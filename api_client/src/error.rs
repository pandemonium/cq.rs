use reqwest::StatusCode;
use std::result::Result as StdResult;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("JSON marshalling failed {0}")]
    Json(#[from] serde_json::Error),

    #[error("HTTP IO failed {0}")]
    Http(#[from] reqwest::Error),

    #[error("Request failed {0}")]
    Server(StatusCode),
}

pub type Result<A> = StdResult<A, Error>;
