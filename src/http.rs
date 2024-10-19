use axum::{
    extract::State,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::{
    application::CommandQueryOrchestrator,
    infrastructure::EventStore,
    model::{self as domain, DomainError, DomainResult},
};

// This thing needs a model into and from which
// the domain types can be mapped
mod model {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    pub struct Author {
        id: domain::AuthorId,
        info: domain::AuthorInfo,
    }

    impl From<domain::Author> for Author {
        fn from(domain::Author(id, info): domain::Author) -> Self {
            Self { id, info }
        }
    }

    impl From<Author> for domain::Author {
        fn from(Author { id, info }: Author) -> Self {
            Self(id, info)
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Book {
        id: domain::BookId,
        info: domain::BookInfo,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct NewBook(pub domain::BookInfo);

    impl From<domain::Book> for Book {
        fn from(domain::Book(id, info): domain::Book) -> Self {
            Self { id, info }
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct NewAuthor(pub domain::AuthorInfo);
}

type Orchestrator<ES> = Arc<CommandQueryOrchestrator<ES>>;
pub struct Api<ES>(Orchestrator<ES>);

impl<ES> Api<ES>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    pub fn new(orchestrator: CommandQueryOrchestrator<ES>) -> Self {
        Self(Arc::new(orchestrator))
    }

    pub async fn start(self, listener: TcpListener) -> DomainResult<()> {
        let Self(orchestrator) = self;
        let routes = routing_configuration().with_state(orchestrator);
        Ok(axum::serve(listener, routes).await?)
    }
}

fn routing_configuration<ES>() -> Router<Orchestrator<ES>>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    let book_routes = Router::new()
        .route("/", get(books::list))
        .route("/", post(books::create));

    let author_routes = Router::new()
        .route("/", get(authors::list))
        .route("/", post(authors::create));

    let api_routes = Router::new()
        .nest("/books", book_routes)
        .nest("/authors", author_routes);

    Router::new()
        .route("/", get(system_root))
        .nest("/api/v1", api_routes)
}

struct ApiError(DomainError);

impl From<DomainError> for ApiError {
    fn from(value: DomainError) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let ApiError(inner) = self;
        format!("{}", inner).into_response()
    }
}

mod books {
    use super::*;
    use crate::{application::QueryAllBooks, model::Command};
    use axum::{http::StatusCode, Json};

    pub async fn list<ES>(
        State(orchestrator): State<Orchestrator<ES>>,
    ) -> Result<Json<Vec<model::Book>>, ApiError>
    where
        ES: EventStore + Send + Sync + Clone + 'static,
    {
        Ok(Json(
            orchestrator
                .issue_query(QueryAllBooks)
                .await?
                .into_iter()
                .map(|b| b.into())
                .collect(),
        ))
    }

    pub async fn create<ES>(
        State(orchestrator): State<Orchestrator<ES>>,
        Json(model::NewBook(book)): Json<model::NewBook>,
    ) -> Result<StatusCode, ApiError>
    where
        ES: EventStore + Send + Sync + Clone + 'static,
    {
        orchestrator.submit_command(Command::AddBook(book)).await;
        Ok(StatusCode::CREATED)
    }
}

mod authors {
    use super::*;
    use crate::{application::QueryAllAuthors, model::Command};
    use axum::{http::StatusCode, Json};

    pub async fn list<ES>(
        State(orchestrator): State<Orchestrator<ES>>,
    ) -> Result<Json<Vec<model::Author>>, ApiError>
    where
        ES: EventStore + Send + Sync + Clone + 'static,
    {
        Ok(Json(
            orchestrator
                .issue_query(QueryAllAuthors)
                .await?
                .into_iter()
                .map(|b| b.into())
                .collect(),
        ))
    }

    pub async fn create<ES>(
        State(orchestrator): State<Orchestrator<ES>>,
        Json(model::NewAuthor(author)): Json<model::NewAuthor>,
    ) -> Result<StatusCode, ApiError>
    where
        ES: EventStore + Send + Sync + Clone + 'static,
    {
        orchestrator
            .submit_command(Command::AddAuthor(author))
            .await;
        Ok(StatusCode::CREATED)
    }
}

async fn system_root<ES>(State(_orchestrator): State<Orchestrator<ES>>) -> Result<String, ApiError>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    Ok("Blister 0.1 running.".to_owned())
}
