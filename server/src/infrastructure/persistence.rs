use std::{path::Path, sync::Arc, time::SystemTime};

use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error,
    infrastructure::{EventDescriptor, ExternalRepresentation, UniqueId},
};

use super::EventStore;

#[derive(Serialize, Deserialize)]
struct ArchivedRepresentation(ExternalRepresentation);

// This does not have to be a reference this way
struct EventId<'a>(&'a Uuid);

impl<'a> AsRef<[u8]> for EventId<'a> {
    fn as_ref(&self) -> &[u8] {
        let Self(id) = self;
        id.as_bytes()
    }
}

// This does not have to be a reference this way
struct AggregateId<'a>(&'a Uuid);

impl<'a> AsRef<[u8]> for AggregateId<'a> {
    fn as_ref(&self) -> &[u8] {
        let Self(id) = self;
        id.as_bytes()
    }
}

impl ArchivedRepresentation {
    fn event_id(&self) -> EventId {
        let Self(ExternalRepresentation { id, .. }) = self;
        EventId(id)
    }

    fn aggregate_id(&self) -> AggregateId {
        let Self(ExternalRepresentation { aggregate_id, .. }) = self;
        AggregateId(aggregate_id)
    }

    fn as_json(&self) -> error::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    fn from_slice(bytes: &[u8]) -> error::Result<Self> {
        Ok(Self(serde_json::from_slice(bytes)?))
    }

    fn into_external_representation(self) -> ExternalRepresentation {
        let Self(it) = self;
        it
    }
}

impl From<ExternalRepresentation> for ArchivedRepresentation {
    fn from(value: ExternalRepresentation) -> Self {
        Self(value)
    }
}

#[derive(Clone)]
pub struct EventArchive(Arc<EventArchiveInner>);

impl EventArchive {
    pub fn try_new<P>(store_path: P) -> error::Result<Self>
    where
        P: AsRef<Path>,
    {
        Ok(Self(Arc::new(EventArchiveInner::try_open(
            Keyspace::open(Config::new(store_path))?,
        )?)))
    }

    fn inner(&self) -> &EventArchiveInner {
        let Self(x) = self;
        x
    }
}

pub struct EventArchiveInner {
    keyspace: Keyspace,
    events: PartitionHandle,
    aggregates: PartitionHandle,
}

impl EventArchiveInner {
    pub fn try_open(keyspace: Keyspace) -> error::Result<Self> {
        let events = keyspace.open_partition("events", PartitionCreateOptions::default())?;
        let aggregates =
            keyspace.open_partition("aggregates", PartitionCreateOptions::default())?;

        Ok(Self {
            keyspace,
            events,
            aggregates,
        })
    }

    fn insert(&self, event: ExternalRepresentation) -> error::Result<()> {
        let mut batch = self.keyspace.batch();

        let archived: ArchivedRepresentation = event.into();
        let primary_key = archived.event_id();

        batch.insert(&self.events, &primary_key, archived.as_json()?);
        batch.insert(&self.aggregates, archived.aggregate_id(), primary_key);

        batch.commit()?;

        // Yes, no, maybe?
        self.keyspace.persist(PersistMode::SyncAll)?;

        Ok(())
    }

    fn find_aggregate_events<'a>(
        &self,
        aggregate_id: AggregateId<'a>,
    ) -> error::Result<Vec<ExternalRepresentation>> {
        let mut events = vec![];

        for pair in self.aggregates.prefix(aggregate_id) {
            let (_, value) = pair?;

            let primary_key = Uuid::from_slice(&value).expect("internal error");
            let Some(event_bytes) = self.events.get(primary_key)? else {
                panic!("corrupt index")
            };

            let archived = ArchivedRepresentation::from_slice(&event_bytes)?;
            events.push(archived.into_external_representation());
        }

        Ok(events)
    }

    fn find_event<'a>(
        &self,
        event_id: EventId<'a>,
    ) -> error::Result<Option<ExternalRepresentation>> {
        if let Some(event_bytes) = self.events.get(&event_id)? {
            let archived = ArchivedRepresentation::from_slice(&event_bytes)?;
            Ok(Some(archived.into_external_representation()))
        } else {
            Ok(None)
        }
    }

    fn find_all(&self) -> error::Result<Vec<ExternalRepresentation>> {
        let mut events = vec![];

        for pair in self.events.iter() {
            let (_, event_bytes) = pair?;
            let archived = ArchivedRepresentation::from_slice(&event_bytes)?;
            events.push(archived.into_external_representation())
        }

        Ok(events)
    }
}

impl EventStore for EventArchive {
    async fn find_by_event_id(
        &self,
        UniqueId(id): UniqueId,
    ) -> error::Result<ExternalRepresentation> {
        if let Some(event) = self.inner().find_event(EventId(&id))? {
            Ok(event)
        } else {
            // I really don't know about this. Why not Option?
            // I guess my thoughts from the get go has been that
            // you will never randomly have an Event Id that may
            // or may not be an actual Event. If you have an ID
            // then there is an Event. Something to think about.
            Err(error::Error::Generic(format!("No such event {id}")))
        }
    }

    async fn find_by_aggregate_id(
        &self,
        UniqueId(id): UniqueId,
    ) -> error::Result<Vec<ExternalRepresentation>> {
        Ok(self.inner().find_aggregate_events(AggregateId(&id))?)
    }

    async fn persist<E>(&mut self, event: E) -> error::Result<()>
    where
        E: EventDescriptor + Send + Sync + 'static,
    {
        let event_id = UniqueId::fresh();
        let event_time = SystemTime::now();
        let event = event.external_representation(event_id, event_time)?;
        self.inner().insert(event)?;

        Ok(())
    }

    async fn journal(&self) -> error::Result<Vec<ExternalRepresentation>> {
        self.inner().find_all()
    }
}

#[cfg(test)]
mod tests {
    use fjall::{Config, PartitionCreateOptions, Result};

    #[test]
    fn xxx() -> Result<()> {
        let keyspace = Config::new("test-keyspace").open()?;
        let _events = keyspace.open_partition("events", PartitionCreateOptions::default())?;

        Ok(())
    }
}
