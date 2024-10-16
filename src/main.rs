use anyhow::{anyhow, Result};
use serde_json::json;
use std::fmt::Debug;
use std::time::SystemTime;
use std::{thread::sleep, time::Duration};
use uuid::{uuid, Uuid};

use blister::{
    application::{CommandQueryOrchestrator, EventBus, QueryBooksByAuthorId, Termination},
    infrastructure::{EventDescriptor, EventStore, ExternalRepresentation, UniqueId},
    model::{AuthorId, BookInfo, Command, Isbn},
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
            .ok_or(anyhow!("No such event"))
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

    async fn persist<E>(&mut self, event: E) -> Result<E>
    where
        E: EventDescriptor,
    {
        let event_id = UniqueId::fresh();

        // does this take place here?
        // Is this the right data type?
        let timestamp = SystemTime::now();

        let event_rep = event.external_representation(event_id, timestamp)?;
        println!("{}", event_rep);

        self.events.push(event_rep);
        Ok(event)
    }

    async fn journal(&self) -> impl Iterator<Item = &ExternalRepresentation> {
        self.events.iter()
    }
}

#[tokio::main]
async fn main() {
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
    let orchestrator = CommandQueryOrchestrator::new(event_bus);

    let termination = Termination::new();
    orchestrator.start(&termination).await;

    orchestrator
        .submit_command(Command::AddBook(BookInfo {
            isbn: Isbn("978-1-61729-961-2".to_owned()),
            title: "Functional Design and Architecture".to_owned(),

            // CommandDispatcher to validate this against its WriteModel
            // to make sure this Author exists -- otherwise it rejects
            // the Command.
            author: AuthorId(UniqueId::fresh()),
        }))
        .await;

    orchestrator
        .submit_command(Command::AddBook(BookInfo {
            isbn: Isbn("978-91-8002498-3".to_owned()),
            title: "Superrika och jämlika".to_owned(),

            // CommandDispatcher to validate this against its WriteModel
            // to make sure this Author exists -- otherwise it rejects
            // the Command.
            author: AuthorId(UniqueId::fresh()),
        }))
        .await;

    // What is the return value of this?
    let books = orchestrator
        .issue_query(QueryBooksByAuthorId(AuthorId(UniqueId(uuid!(
            "ba68afbe-83a7-4a5e-9619-8a32a8967b28"
        )))))
        .await
        .expect("Where is my book?");

    println!("Books: {books:?}");

    sleep(Duration::from_secs(2));

    termination.signal();
}
