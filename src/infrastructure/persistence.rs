use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{error, infrastructure::ExternalRepresentation};

#[derive(Serialize, Deserialize)]
struct ArchivedRepresentation(ExternalRepresentation);

struct Key<'a>(&'a Uuid);

impl<'a> AsRef<[u8]> for Key<'a> {
    fn as_ref(&self) -> &[u8] {
        let Self(id) = self;
        id.as_bytes()
    }
}

struct AggregateKey<'a>(&'a Uuid);

impl<'a> AsRef<[u8]> for AggregateKey<'a> {
    fn as_ref(&self) -> &[u8] {
        let Self(id) = self;
        id.as_bytes()
    }
}

impl ArchivedRepresentation {
    fn event_id(&self) -> Key {
        let Self(ExternalRepresentation { id, .. }) = self;
        Key(id)
    }

    fn aggregate_id(&self) -> AggregateKey {
        let Self(ExternalRepresentation { aggregate_id, .. }) = self;
        AggregateKey(aggregate_id)
    }

    fn as_json(&self) -> error::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    fn from_slice(bytes: &[u8]) -> error::Result<Self> {
        Ok(Self(serde_json::from_slice(bytes)?))
    }

    fn into_external_representation(self) -> ExternalRepresentation {
        self.0
    }
}

impl From<ExternalRepresentation> for ArchivedRepresentation {
    fn from(value: ExternalRepresentation) -> Self {
        Self(value)
    }
}

struct EventArchive {
    keyspace: Keyspace,
    events: PartitionHandle,
    aggregates: PartitionHandle,
}

impl EventArchive {
    fn try_open(keyspace: Keyspace) -> error::Result<Self> {
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
        aggregate_id: AggregateKey<'a>,
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

    fn find_event<'a>(&self, event_id: Key<'a>) -> error::Result<Option<ExternalRepresentation>> {
        if let Some(event_bytes) = self.events.get(&event_id)? {
            let archived = ArchivedRepresentation::from_slice(&event_bytes)?;
            Ok(Some(archived.into_external_representation()))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use fjall::{Config, PartitionCreateOptions, Result};

    #[test]
    fn xxx() -> Result<()> {
        let keyspace = Config::new("test-keyspace").open()?;
        let events = keyspace.open_partition("events", PartitionCreateOptions::default())?;

        Ok(())
    }
}
