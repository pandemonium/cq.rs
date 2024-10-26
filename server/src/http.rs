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
        Application,
    },
    error::{Error, Result},
    infrastructure::EventStore,
};

pub mod model;

const API_RESOURCE_PREFIX: &str = "/api/v1";

type ApiResult<A> = StdResult<A, ApiError>;

// The Api type can go away and become just a function:
// http::start_api(application)
type ApplicationInner<ES> = Arc<Application<ES>>;
pub struct Api<ES>(ApplicationInner<ES>);

impl<ES> Api<ES>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    pub fn new(application: Application<ES>) -> Self {
        Self(Arc::new(application))
    }

    pub async fn start(self, listener: TcpListener) -> Result<()> {
        let Self(application) = self;
        let routes = routing_configuration().with_state(application);
        Ok(axum::serve(listener, routes).await?)
    }
}

fn routing_configuration<ES>() -> Router<ApplicationInner<ES>>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    let books = Router::new()
        .route("/", get(books::list))
        .route("/", post(books::create))
        .route("/:id", get(books::get))
        .route("/:id/author", get(authors::by_book)); // todo: 'authors' and change the
                                                      // model tor reflect this

    let authors = Router::new()
        .route("/", get(authors::list))
        .route("/", post(authors::create))
        .route("/:id", get(authors::get))
        .route("/:id/books", get(books::by_author));

    let search = get(search::text);

    let api = Router::new()
        .nest("/books", books)
        .nest("/authors", authors)
        .route("/search", search);

    Router::new()
        .route("/", get(system_root))
        .nest(API_RESOURCE_PREFIX, api)
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

mod search {
    use super::*;
    use axum::{extract::Query, Json};

    use domain::query;

    pub async fn text<ES>(
        State(application): State<ApplicationInner<ES>>,
        Query(model::SearchTerm { query }): Query<model::SearchTerm>,
    ) -> ApiResult<Json<Vec<model::SearchResultItem>>>
    where
        ES: EventStore + Clone + 'static,
    {
        let hits = application
            .issue_query(query::text::SearchQuery(query))
            .await?
            .into_iter()
            .map(|hit| model::SearchResultItem::from_search_hit(hit.into(), API_RESOURCE_PREFIX))
            .collect();

        Ok(Json(hits))
    }
}

mod books {
    use super::*;
    use axum::{extract::Path, http::StatusCode, Json};

    use domain::{query, Command};

    pub async fn get<ES>(
        State(application): State<ApplicationInner<ES>>,
        Path(model::BookId(book_id)): Path<model::BookId>,
    ) -> ApiResult<Json<model::Book>>
    where
        ES: EventStore + Clone + 'static,
    {
        if let Some(book) = application.issue_query(query::BookById(book_id)).await? {
            Ok(Json(book.into()))
        } else {
            ApiError::not_found()
        }
    }

    pub async fn list<ES>(
        State(application): State<ApplicationInner<ES>>,
    ) -> ApiResult<Json<Vec<model::Book>>>
    where
        ES: EventStore + Clone + 'static,
    {
        Ok(Json(
            application
                .issue_query(query::AllBooks)
                .await?
                .into_iter()
                .map(|b| b.into())
                .collect(),
        ))
    }

    // return a URI to the created resource
    pub async fn create<ES>(
        State(application): State<ApplicationInner<ES>>,
        Json(model::NewBook(book)): Json<model::NewBook>,
    ) -> ApiResult<StatusCode>
    where
        ES: EventStore + Clone + 'static,
    {
        if application.submit_command(Command::AddBook(book)).await {
            Ok(StatusCode::CREATED)
        } else {
            Ok(StatusCode::NOT_ACCEPTABLE)
        }
    }

    pub async fn by_author<ES>(
        State(application): State<ApplicationInner<ES>>,
        Path(model::AuthorId(author_id)): Path<model::AuthorId>,
    ) -> ApiResult<Json<Vec<model::Book>>>
    where
        ES: EventStore + Clone + 'static,
    {
        Ok(Json(
            application
                .issue_query(query::BooksByAuthorId(author_id))
                .await?
                .into_iter()
                .map(|b| b.into())
                .collect(),
        ))
    }
}

mod authors {
    use super::*;
    use axum::{extract::Path, http::StatusCode, Json};

    use domain::{query, Command};

    pub async fn get<ES>(
        State(application): State<ApplicationInner<ES>>,
        Path(model::AuthorId(author_id)): Path<model::AuthorId>,
    ) -> ApiResult<Json<model::Author>>
    where
        ES: EventStore + Clone + 'static,
    {
        if let Some(author) = application
            .issue_query(query::AuthorById(author_id))
            .await?
        {
            Ok(Json(author.into()))
        } else {
            ApiError::not_found()
        }
    }

    pub async fn list<ES>(
        State(application): State<ApplicationInner<ES>>,
    ) -> ApiResult<Json<Vec<model::Author>>>
    where
        ES: EventStore + Clone + 'static,
    {
        Ok(Json(
            application
                .issue_query(query::AllAuthors)
                .await?
                .into_iter()
                .map(|b| b.into())
                .collect(),
        ))
    }

    // return a URI to the created resource
    pub async fn create<ES>(
        State(application): State<ApplicationInner<ES>>,
        Json(model::NewAuthor(author)): Json<model::NewAuthor>,
    ) -> ApiResult<StatusCode>
    where
        ES: EventStore + Clone + 'static,
    {
        application.submit_command(Command::AddAuthor(author)).await;
        Ok(StatusCode::CREATED)
    }

    pub async fn by_book<ES>(
        State(application): State<ApplicationInner<ES>>,
        Path(model::BookId(book_id)): Path<model::BookId>,
    ) -> ApiResult<Json<model::Author>>
    where
        ES: EventStore + Clone + 'static,
    {
        if let Some(author) = application
            .issue_query(query::AuthorByBookId(book_id))
            .await?
        {
            Ok(Json(author.into()))
        } else {
            ApiError::not_found()
        }
    }
}

async fn system_root<ES>(State(_application): State<ApplicationInner<ES>>) -> ApiResult<String>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    Ok("Blister 0.1 running.".to_owned())
}