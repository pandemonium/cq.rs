use std::io::Read;

use fjall::*;
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
    fn key(&self) -> Key {
        let Self(ExternalRepresentation { id, .. }) = self;
        Key(id)
    }

    fn aggregate_key(&self) -> AggregateKey {
        let Self(ExternalRepresentation { aggregate_id, .. }) = self;
        AggregateKey(aggregate_id)
    }

    fn json(&self) -> error::Result<String> {
        Ok(serde_json::to_string(self)?)
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

    // some of these things might be blocking - what does that mean?
    async fn insert(&self, event: ExternalRepresentation) -> error::Result<()> {
        let event: ArchivedRepresentation = event.into();
        let key = event.key();

        let mut batch = self.keyspace.batch();

        batch.insert(&self.events, &key, event.json()?);
        batch.insert(&self.aggregates, event.aggregate_key(), key);

        batch.commit()?;

        Ok(())
    }

    async fn find_aggregate_events<'a>(
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

            events.push(serde_json::from_slice(&event_bytes)?);
        }

        Ok(events)
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
