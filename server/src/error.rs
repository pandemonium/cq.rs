use std::io;

use axum::http::header::InvalidHeaderValue;
use thiserror::Error;
use tokio::sync::broadcast::error::RecvError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to marshall json data {0}")]
    Json(#[from] serde_json::Error),

    #[error("Unknown event-type `{0}`")]
    UnknownEventType(String),

    #[error("Failed to parse event data {0}")]
    AggregateParseError(String),

    #[error("IO error {0}")]
    IoError(#[from] io::Error),

    #[error("Axum HTTP error {0}")]
    AxumHttp(#[from] InvalidHeaderValue),

    #[error("Error receving an event {0}")]
    ReceiveError(#[from] RecvError),

    #[error("Generic error {0}")]
    Generic(String),

    #[error("Regex error {0}")]
    Regex(#[from] regex::Error),

    #[error("Fjall persistence error {0}")]
    EventArchive(#[from] fjall::Error),
}

pub type Result<A> = std::result::Result<A, Error>;
