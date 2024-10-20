use std::io;

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

    #[error("Error receving an event {0}")]
    ReceiveError(#[from] RecvError),

    #[error("Generic error {0}")]
    Generic(String),
}

pub type Result<A> = std::result::Result<A, Error>;
