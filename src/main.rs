use serde_json::json;
use std::fmt::Debug;
use std::time::SystemTime;
use tokio::net::TcpListener;
use uuid::Uuid;

use blister::{
    core::{CommandQueryOrchestrator, EventBus},
    error::{Error, Result},
    http,
    infrastructure::{EventDescriptor, EventStore, ExternalRepresentation, Termination, UniqueId},
};

#[derive(Clone, Debug, Default)]
struct DummyStore {
    events: Vec<ExternalRepresentation>,
}

impl DummyStore {}

impl EventStore for DummyStore {
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

    async fn journal(&self) -> impl Iterator<Item = &ExternalRepresentation> {
        self.events.iter()
    }
}

fn make_orchestrator() -> CommandQueryOrchestrator<DummyStore> {
    let store = DummyStore {
        events: vec![ExternalRepresentation {
            id: Uuid::new_v4(),
            when: SystemTime::now(),
            aggregate_id: Uuid::new_v4(),
            what: "book-added".to_owned(),
            data: json!({"author":"ba68afbe-83a7-4a5e-9619-8a32a8967b28","isbn":"978-1-61180-697-7","title":"The Art of War"}),
        }],
    };
    let event_bus = EventBus::new(store);
    CommandQueryOrchestrator::new(event_bus)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let listener = TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("a free port");

    let orchestrator = make_orchestrator();

    let terminator = Termination::new();
    orchestrator.start(&terminator).await;

    http::Api::new(orchestrator)
        .start(listener)
        .await
        .expect("starting the API to work");

    terminator.signal();
}
