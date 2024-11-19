use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    fmt::{self, Display},
    future::Future,
    sync::Arc,
    time::SystemTime,
};
use time::OffsetDateTime;
use tokio::sync::{broadcast, Mutex};
use uuid::Uuid;

use crate::error::{Error, Result};

pub mod persistence;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueId(pub Uuid);

impl UniqueId {
    pub fn fresh() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn uuid(&self) -> &Uuid {
        let Self(id) = self;
        id
    }
}

impl Display for UniqueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let UniqueId(id) = self;
        write!(f, "{id}")
    }
}

#[derive(Clone)]
pub struct Termination {
    signal: broadcast::Sender<()>,
}

impl Termination {
    pub fn new() -> Self {
        let (signal, _rx) = broadcast::channel(1);
        Self { signal }
    }

    pub fn waiter(&self) -> TerminationWaiter {
        TerminationWaiter::new(self.signal.subscribe())
    }

    pub fn signal(&self) {
        self.signal.send(()).expect("msg");
    }
}

#[derive(Clone)]
pub struct TerminationWaiter(Arc<Mutex<broadcast::Receiver<()>>>);

impl TerminationWaiter {
    fn new(receiver: broadcast::Receiver<()>) -> Self {
        Self(Arc::new(Mutex::new(receiver)))
    }

    pub async fn wait(&self) {
        self.0.lock().await.recv().await.expect("wtf")
    }
}

pub trait EventStore {
    async fn find_by_event_id(&self, id: UniqueId) -> Result<ExternalRepresentation>;
    async fn find_by_aggregate_id(&self, id: UniqueId) -> Result<Vec<ExternalRepresentation>>;

    async fn load_aggregate<Aggregate>(&self, aggregate: Aggregate) -> Result<Aggregate::Root>
    where
        Aggregate: AggregateIdentity,
    {
        let stream = self.find_by_aggregate_id(*aggregate.id()).await?;
        Aggregate::Root::try_load(AggregateStream(stream))
    }

    // Use internal mutability instead?
    // This function has to be this way because the Future has to be Send
    // I wonder if this is something I can solve some other way because this
    // is not pretty. I must be doing something wrong.
    fn persist<E>(&mut self, event: E) -> impl Future<Output = Result<()>> + Send
    where
        E: EventDescriptor + Send + Sync + 'static;

    // This is a pourly thought out solution for journal replays
    async fn journal(&self) -> Result<Vec<ExternalRepresentation>>;
}

pub trait EventDescriptor: Sized {
    fn external_representation(
        &self,
        event_id: UniqueId,
        event_time: SystemTime,
    ) -> Result<ExternalRepresentation>;

    fn from_external_representation(external: &ExternalRepresentation) -> Result<Self>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExternalRepresentation {
    pub id: Uuid,
    pub when: SystemTime,
    pub aggregate_id: Uuid,
    pub what: String,
    pub data: JsonValue,
}

impl Display for ExternalRepresentation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ExternalRepresentation {
            id,
            when,
            aggregate_id,
            what,
            data,
        } = self;

        let when: OffsetDateTime = (*when).into();
        writeln!(f, "[{when}] {aggregate_id}/{id} {what}")?;

        let data = serde_json::to_string(data).expect("trust serde");
        writeln!(f, "{data}")
    }
}

// Can I combine AggregateRoot with AggregateIdentity?
pub trait AggregateRoot: Sized {
    type Id: AggregateIdentity;

    fn try_load(stream: AggregateStream) -> Result<Self>;
}

pub trait AggregateIdentity {
    type Root: AggregateRoot<Id = Self>;

    fn id(&self) -> &UniqueId;
}

pub struct AggregateStream(pub Vec<ExternalRepresentation>);

impl AggregateStream {
    pub fn peek<E>(&self) -> Result<E>
    where
        E: EventDescriptor,
    {
        E::from_external_representation(
            self.0
                .first()
                .ok_or(Error::Generic("expected an event".to_owned()))?,
        )
    }
}
