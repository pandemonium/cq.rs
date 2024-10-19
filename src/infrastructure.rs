use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    fmt::{self, Display},
    time::SystemTime,
};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::model::{DomainError, DomainResult};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueId(pub Uuid);

impl UniqueId {
    pub fn fresh() -> Self {
        Self(Uuid::new_v4())
    }
}

pub trait EventStore {
    async fn find_by_event_id(&self, id: UniqueId) -> DomainResult<ExternalRepresentation>;
    async fn find_by_aggregate_id(&self, id: UniqueId)
        -> DomainResult<Vec<ExternalRepresentation>>;

    async fn load_aggregate<Aggregate>(&self, aggregate: Aggregate) -> DomainResult<Aggregate::Root>
    where
        Aggregate: AggregateIdentity,
    {
        let stream = self.find_by_aggregate_id(*aggregate.id()).await?;
        Aggregate::Root::try_load(AggregateStream(stream))
    }

    // Use internal mutability instead?
    fn persist<E>(
        &mut self,
        event: E,
    ) -> impl std::future::Future<Output = DomainResult<()>> + Send
    where
        E: EventDescriptor + Send + Sync + 'static;

    async fn journal(&self) -> impl Iterator<Item = &ExternalRepresentation>;
}

pub trait EventDescriptor: Sized {
    fn external_representation(
        &self,
        event_id: UniqueId,
        event_time: SystemTime,
    ) -> DomainResult<ExternalRepresentation>;
    fn from_external_representation(external: &ExternalRepresentation) -> DomainResult<Self>;
}

#[derive(Clone, Debug)]
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

    fn try_load(stream: AggregateStream) -> DomainResult<Self>;
}

pub trait AggregateIdentity {
    type Root: AggregateRoot<Id = Self>;

    fn id(&self) -> &UniqueId;
}

pub struct AggregateStream(pub Vec<ExternalRepresentation>);

impl AggregateStream {
    pub fn peek<E>(&self) -> DomainResult<E>
    where
        E: EventDescriptor,
    {
        Ok(E::from_external_representation(self.0.first().ok_or(
            DomainError::Generic("expected an event".to_owned()),
        )?)?)
    }
}
