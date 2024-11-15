use axum::{
    extract::Path,
    extract::Query,
    extract::State,
    http::StatusCode,
    http::{HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;
use std::{result::Result as StdResult, sync::Arc};
use tokio::net::TcpListener;
use uuid::Uuid;

use crate::{
    core::{
        model::{self as domain},
        Application, CommandReceipt,
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
        .route("/:id/readers", post(books::add_reader))
        .route("/:id/author", get(authors::by_book)); // todo: 'authors' and change the
                                                      // model tor reflect this

    let authors = Router::new()
        .route("/", get(authors::list))
        .route("/", post(authors::create))
        .route("/:id", get(authors::get))
        .route("/:id/books", get(books::by_author));

    let readers = Router::new()
        .route("/", get(readers::list))
        .route("/", post(readers::create))
        .route("/moniker/:moniker", get(readers::by_unique_moniker))
        .route("/:id", get(readers::get))
        .route("/:id/books", get(books::by_reader));

    let search = get(search::text);

    let api = Router::new()
        .nest("/books", books)
        .nest("/authors", authors)
        .nest("/readers", readers)
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

#[derive(Serialize)]
struct Resource {
    id: Uuid,

    #[serde(skip)]
    inner: domain::ResourceId,
}

impl Resource {
    fn location(&self) -> String {
        resource_location(
            match self.inner {
                domain::ResourceId::Author(..) => "authors",
                domain::ResourceId::Book(..) => "books",
                domain::ResourceId::Reader(..) => "readers",
            },
            &self.id.to_string(),
        )
    }
}

impl From<domain::ResourceId> for Resource {
    fn from(inner: domain::ResourceId) -> Self {
        let id = match inner {
            domain::ResourceId::Book(id) => id.into(),
            domain::ResourceId::Author(id) => id.into(),
            domain::ResourceId::Reader(id) => id.into(),
        };

        Self { id, inner }
    }
}

fn created_response(resource: Resource) -> ApiResult<(StatusCode, HeaderMap, Json<Resource>)> {
    let mut headers = HeaderMap::default();
    headers.insert(
        "Location",
        HeaderValue::from_str(&resource.location()).map_err(Error::from)?,
    );
    Ok((StatusCode::CREATED, headers, Json(resource)))
}

// I would like this to have the correct URL
fn resource_location(resource_type: &str, id: &str) -> String {
    format!("{}/{resource_type}/{id}", API_RESOURCE_PREFIX)
}

impl From<CommandReceipt> for ApiResult<Response> {
    fn from(value: CommandReceipt) -> Self {
        if let CommandReceipt::Created(id) = value {
            Ok(created_response(id.into())?.into_response())
        } else {
            Ok(StatusCode::NOT_ACCEPTABLE.into_response())
        }
    }
}

mod search {
    use super::*;

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
    ) -> ApiResult<Response>
    where
        ES: EventStore + Clone + 'static,
    {
        application
            .submit_command(Command::AddBook(book))
            .await
            .into()
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

    pub async fn by_reader<ES>(
        State(application): State<ApplicationInner<ES>>,
        Path(model::ReaderId(reader_id)): Path<model::ReaderId>,
    ) -> ApiResult<Json<Vec<model::Book>>>
    where
        ES: EventStore + Clone + 'static,
    {
        Ok(Json(
            application
                .issue_query(query::BooksByReader(reader_id))
                .await?
                .into_iter()
                .map(|b| b.into())
                .collect(),
        ))
    }

    pub async fn add_reader<ES>(
        State(application): State<ApplicationInner<ES>>,
        Path(model::BookId(book_id)): Path<model::BookId>,
        Json(model::NewBookRead { reader_id, when }): Json<model::NewBookRead>,
    ) -> ApiResult<StatusCode>
    where
        ES: EventStore + Clone + 'static,
    {
        if application
            .submit_command(Command::AddReadBook(domain::BookReadInfo {
                reader_id: reader_id.into(),
                book_id,
                when,
            }))
            .await
            .is_success()
        {
            Ok(StatusCode::ACCEPTED)
        } else {
            Ok(StatusCode::NOT_ACCEPTABLE)
        }
    }
}

mod authors {
    use super::*;

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
    ) -> ApiResult<Response>
    where
        ES: EventStore + Clone + 'static,
    {
        application
            .submit_command(Command::AddAuthor(author))
            .await
            .into()
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

mod readers {
    use super::*;

    use domain::{query, Command};

    pub async fn get<ES>(
        State(application): State<ApplicationInner<ES>>,
        Path(model::ReaderId(reader_id)): Path<model::ReaderId>,
    ) -> ApiResult<Json<model::Reader>>
    where
        ES: EventStore + Clone + 'static,
    {
        if let Some(reader) = application
            .issue_query(query::ReaderById(reader_id))
            .await?
        {
            Ok(Json(reader.into()))
        } else {
            ApiError::not_found()
        }
    }

    pub async fn list<ES>(
        State(application): State<ApplicationInner<ES>>,
    ) -> ApiResult<Json<Vec<model::Reader>>>
    where
        ES: EventStore + Clone + 'static,
    {
        Ok(Json(
            application
                .issue_query(query::AllReaders)
                .await?
                .into_iter()
                .map(|b| b.into())
                .collect(),
        ))
    }

    pub async fn create<ES>(
        State(application): State<ApplicationInner<ES>>,
        Json(model::NewReader(reader)): Json<model::NewReader>,
    ) -> ApiResult<Response>
    where
        ES: EventStore + Clone + 'static,
    {
        application
            .submit_command(Command::AddReader(reader))
            .await
            .into()
    }

    pub async fn by_unique_moniker<ES>(
        State(application): State<ApplicationInner<ES>>,
        Path(moniker): Path<String>,
    ) -> ApiResult<Json<Option<model::Reader>>>
    where
        ES: EventStore + Clone + 'static,
    {
        Ok(Json(
            application
                .issue_query(query::UniqueReaderByMoniker(moniker))
                .await?
                .map(|b| b.into()),
        ))
    }
}

async fn system_root<ES>(State(_application): State<ApplicationInner<ES>>) -> ApiResult<String>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    Ok("Blister 0.1 running.".to_owned())
}
