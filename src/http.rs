use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use std::{result::Result as StdResult, sync::Arc};
use tokio::net::TcpListener;

use crate::{
    core::{
        model::{self as domain},
        CommandQueryOrchestrator,
    },
    error::{Error, Result},
    infrastructure::EventStore,
};

pub mod model;

type ApiResult<A> = StdResult<A, ApiError>;

type Orchestrator<ES> = Arc<CommandQueryOrchestrator<ES>>;
pub struct Api<ES>(Orchestrator<ES>);

impl<ES> Api<ES>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    pub fn new(orchestrator: CommandQueryOrchestrator<ES>) -> Self {
        Self(Arc::new(orchestrator))
    }

    pub async fn start(self, listener: TcpListener) -> Result<()> {
        let Self(orchestrator) = self;
        let routes = routing_configuration().with_state(orchestrator);
        Ok(axum::serve(listener, routes).await?)
    }
}

fn routing_configuration<ES>() -> Router<Orchestrator<ES>>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    let books = Router::new()
        .route("/", get(books::list))
        .route("/", post(books::create))
        .route("/:id", get(books::get));

    let authors = Router::new()
        .route("/", get(authors::list))
        .route("/", post(authors::create))
        .route("/:id", get(authors::get));

    let api = Router::new()
        .nest("/books", books)
        .nest("/authors", authors);

    Router::new()
        .route("/", get(system_root))
        .nest("/api/v1", api)
}

enum ApiError {
    Internal(Error),
    ServiceStatus(StatusCode),
}

impl ApiError {
    fn not_found<A>() -> ApiResult<A> {
        Err(ApiError::ServiceStatus(StatusCode::NOT_FOUND))
    }
}

impl From<Error> for ApiError {
    fn from(value: Error) -> Self {
        Self::Internal(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::Internal(error) => format!("{error}").into_response(),
            ApiError::ServiceStatus(status) => status.into_response(),
        }
    }
}

mod books {
    use super::*;
    use axum::{extract::Path, http::StatusCode, Json};

    use domain::{query, Command};

    pub async fn get<ES>(
        State(orchestrator): State<Orchestrator<ES>>,
        Path(model::BookId(book_id)): Path<model::BookId>,
    ) -> ApiResult<Json<model::Book>>
    where
        ES: EventStore + Clone + 'static,
    {
        if let Some(book) = orchestrator.issue_query(query::BookById(book_id)).await? {
            Ok(Json(book.into()))
        } else {
            ApiError::not_found()
        }
    }

    pub async fn list<ES>(
        State(orchestrator): State<Orchestrator<ES>>,
    ) -> ApiResult<Json<Vec<model::Book>>>
    where
        ES: EventStore + Clone + 'static,
    {
        Ok(Json(
            orchestrator
                .issue_query(query::AllBooks)
                .await?
                .into_iter()
                .map(|b| b.into())
                .collect(),
        ))
    }

    // return a URI to the created resource
    pub async fn create<ES>(
        State(orchestrator): State<Orchestrator<ES>>,
        Json(model::NewBook(book)): Json<model::NewBook>,
    ) -> ApiResult<StatusCode>
    where
        ES: EventStore + Clone + 'static,
    {
        if orchestrator.submit_command(Command::AddBook(book)).await {
            Ok(StatusCode::CREATED)
        } else {
            Ok(StatusCode::NOT_ACCEPTABLE)
        }
    }
}

mod authors {
    use super::*;
    use axum::{extract::Path, http::StatusCode, Json};

    use domain::{query, Command};

    pub async fn get<ES>(
        State(orchestrator): State<Orchestrator<ES>>,
        Path(model::BookId(book_id)): Path<model::BookId>,
    ) -> ApiResult<Json<model::Book>>
    where
        ES: EventStore + Clone + 'static,
    {
        if let Some(book) = orchestrator.issue_query(query::BookById(book_id)).await? {
            Ok(Json(book.into()))
        } else {
            ApiError::not_found()
        }
    }

    pub async fn list<ES>(
        State(orchestrator): State<Orchestrator<ES>>,
    ) -> ApiResult<Json<Vec<model::Author>>>
    where
        ES: EventStore + Clone + 'static,
    {
        Ok(Json(
            orchestrator
                .issue_query(query::AllAuthors)
                .await?
                .into_iter()
                .map(|b| b.into())
                .collect(),
        ))
    }

    // return a URI to the created resource
    pub async fn create<ES>(
        State(orchestrator): State<Orchestrator<ES>>,
        Json(model::NewAuthor(author)): Json<model::NewAuthor>,
    ) -> ApiResult<StatusCode>
    where
        ES: EventStore + Clone + 'static,
    {
        orchestrator
            .submit_command(Command::AddAuthor(author))
            .await;
        Ok(StatusCode::CREATED)
    }
}

async fn system_root<ES>(State(_orchestrator): State<Orchestrator<ES>>) -> ApiResult<String>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    Ok("Blister 0.1 running.".to_owned())
}
