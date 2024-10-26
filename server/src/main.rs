use serde_json::json;
use std::time::SystemTime;
use std::{fmt::Debug, path::Path};
use tokio::net::TcpListener;
use uuid::Uuid;

use server::{
    core::{Application, EventBus},
    error::{Error, Result},
    http,
    infrastructure::{
        persistence::EventArchive, EventDescriptor, EventStore, ExternalRepresentation,
        Termination, UniqueId,
    },
};

#[derive(Clone, Debug, Default)]
struct _DummyStore {
    events: Vec<ExternalRepresentation>,
}

impl _DummyStore {}

impl EventStore for _DummyStore {
    async fn find_by_event_id(&self, UniqueId(id): UniqueId) -> Result<ExternalRepresentation> {
        self.events
            .iter()
            .find(|e| e.id == id)
            .ok_or(Error::Generic("No such event".to_owned()))
            .cloned()
    }

    async fn find_by_aggregate_id(
        &self,
        UniqueId(id): UniqueId,
    ) -> Result<Vec<ExternalRepresentation>> {
        Ok(self
            .events
            .iter()
            .filter(|e| e.aggregate_id == id)
            .cloned()
            .collect())
    }

    async fn persist<E>(&mut self, event: E) -> Result<()>
    where
        E: EventDescriptor + Send + Sync + 'static,
    {
        let event_id = UniqueId::fresh();

        // does this take place here?
        // Is this the right data type?
        let timestamp = SystemTime::now();

        let event_rep = event.external_representation(event_id, timestamp)?;
        self.events.push(event_rep);
        Ok(())
    }

    async fn journal(&self) -> Result<Vec<ExternalRepresentation>> {
        Ok(self.events.clone())
    }
}

fn _make_application() -> Application<_DummyStore> {
    let store = _DummyStore {
        events: vec![ExternalRepresentation {
            id: Uuid::new_v4(),
            when: SystemTime::now(),
            aggregate_id: Uuid::new_v4(),
            what: "book-added".to_owned(),
            data: json!({"author":"ba68afbe-83a7-4a5e-9619-8a32a8967b28","isbn":"978-1-61180-697-7","title":"The Art of War"}),
        }],
    };
    let event_bus = EventBus::new(store);
    Application::new(event_bus)
}

fn make_application<P>(store_path: P) -> Application<EventArchive>
where
    P: AsRef<Path>,
{
    let event_store = EventArchive::try_new(store_path).expect("a valid event archive");
    let event_bus = EventBus::new(event_store);

    Application::new(event_bus)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let listener = TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("a free port");

    let application = make_application("event-store");

    let terminator = Termination::new();
    // threaded because both the QueryHandler and CommandDispatcher
    // both poll for events
    // I guess these parts could be re-written to be event driven instead
    application.start(&terminator).await;

    http::Api::new(application)
        .start(listener)
        .await
        .expect("starting the API to work");

    terminator.signal();
}
