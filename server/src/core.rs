use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};
use tokio::{
    sync::{
        broadcast::{self, Receiver, Sender},
        Mutex, RwLock,
    },
    task,
};

use crate::{
    error::{Error, Result},
    infrastructure::{EventDescriptor, EventStore, Termination, TerminationWaiter, UniqueId},
};
use model::{query, AuthorId, BookId, Command, Event, ReaderId};

pub mod model;

struct CommandDispatcher<ES> {
    event_bus: EventBus<ES, Event>,
    write_model: Arc<RwLock<WriteModel>>,
}

impl<ES> CommandDispatcher<ES>
where
    ES: EventStore,
{
    fn new(event_bus: EventBus<ES, Event>) -> Self {
        Self {
            event_bus,
            write_model: Default::default(),
        }
    }

    async fn start(&self, terminate: TerminationWaiter) -> task::JoinHandle<()> {
        let events = self.event_bus.subscribe();
        let write_model = Arc::clone(&self.write_model);

        // Is there a race condition between this and the ReadModel subscriber?
        self.event_bus
            .replay_journal()
            .await
            .expect("a working replay");

        task::spawn(async move {
            // Can this be inverted somehow? No.
            // The loop would  have to have a select in its body
            // that inspects the terminate condition. Right?
            loop {
                tokio::select! {
                    event = events.poll() => {
                        if let Ok(event) = event {
                            write_model.write().await.apply(event)
                        } else {
                            break
                        }
                    }
                    _ = terminate.wait() => {
                        break;
                    }
                }
            }
        })
    }

    async fn accept(&self, command: Command) -> bool {
        match command {
            Command::AddBook(info) => {
                // Can this be transplanted onto a Book aggregate
                // type? It would have: create(id) and emit events.
                // Or does it need to look stuff up so that that
                // won't work very well?
                if self
                    .write_model
                    .read()
                    .await
                    .author_ids
                    .contains(&info.author)
                {
                    let id = BookId(UniqueId::fresh());
                    self.event_bus
                        .emit(Event::BookAdded(id, info))
                        .await
                        .expect("emit");
                    true
                } else {
                    false
                }
            }
            Command::AddAuthor(info) => {
                let id = AuthorId(UniqueId::fresh());
                self.event_bus
                    .emit(Event::AuthorAdded(id, info))
                    .await
                    .expect("emit");
                true
            }
            Command::AddReader(info) => {
                if self
                    .write_model
                    .read()
                    .await
                    .reader_id_by_moniker
                    .get(&info.unique_moniker)
                    .is_none()
                {
                    let id = ReaderId(UniqueId::fresh());
                    self.event_bus
                        .emit(Event::ReaderAdded(id, info))
                        .await
                        .expect("emit");
                    true
                } else {
                    false
                }
            }
            Command::AddReadBook(info) => {
                if !self
                    .write_model
                    .read()
                    .await
                    .books_read
                    .get(&info.reader_id)
                    .is_some_and(|books| books.contains(&info.book_id))
                {
                    self.event_bus
                        .emit(Event::BookRead(info.reader_id, info))
                        .await
                        .expect("emit");
                    true
                } else {
                    false
                }
            }
        }
    }
}

struct QueryHandler {
    read_model: Arc<RwLock<query::IndexSet>>,
    event_source: Arc<EventBusSubscription<Event>>,
}

impl QueryHandler {
    fn new(subscription: EventBusSubscription<Event>) -> Self {
        Self {
            read_model: Default::default(),
            event_source: Arc::new(subscription),
        }
    }

    fn start(&self, termination: TerminationWaiter) -> task::JoinHandle<()> {
        let read_model = Arc::clone(&self.read_model);
        let event_source = Arc::clone(&self.event_source);

        task::spawn(async move {
            loop {
                tokio::select! {
                    // is it necessary to have this wrapper? It looks better
                    // but causes a Mutex
                    event = event_source.poll() => {
                        if let Ok(event) = event {
                            read_model.write().await.apply(event)
                        } else {
                            break
                        }
                    }
                    _ = termination.wait() => { break }
                }
            }
        })
    }

    async fn issue<Q>(&self, query: Q) -> Result<Q::Output>
    where
        Q: query::IndexSetQuery,
    {
        let read_model = self.read_model.read().await;
        Ok(query.execute(&read_model))
    }
}

pub struct Application<ES> {
    command_dispatcher: CommandDispatcher<ES>,
    query_handler: QueryHandler,
}

impl<ES> Application<ES>
where
    ES: EventStore,
{
    pub fn new(event_bus: EventBus<ES, Event>) -> Self {
        let event_subscription = event_bus.subscribe();
        Application {
            command_dispatcher: CommandDispatcher::new(event_bus),
            query_handler: QueryHandler::new(event_subscription),
        }
    }

    pub async fn start(&self, termination: &Termination) {
        let waiter = termination.waiter();
        tokio::select! {
            _ = self.command_dispatcher.start(termination.waiter()) => {}
            _ = self.query_handler.start(termination.waiter()) => {}
            _ = waiter.wait() => {}
        }
    }

    pub async fn issue_query<Q>(&self, query: Q) -> Result<Q::Output>
    where
        Q: query::IndexSetQuery,
    {
        self.query_handler.issue(query).await
    }

    // Should be Result<(), ValidationError>
    pub async fn submit_command(&self, command: Command) -> bool {
        self.command_dispatcher.accept(command).await
    }
}

// This has to lose the EventStore.
// But can I make this know about the concrete event type?
pub struct EventBus<ES, E> {
    event_store: Mutex<ES>,
    tx: Sender<E>,
}

impl<ES, E> EventBus<ES, E>
where
    ES: EventStore,
    E: EventDescriptor + Sync + Send + Clone + fmt::Debug + 'static,
{
    pub fn new(event_store: ES) -> Self {
        let (tx, _rx) = broadcast::channel(100);
        Self {
            event_store: Mutex::new(event_store),
            tx,
        }
    }

    async fn replay_journal(&self) -> Result<()> {
        for record in self.event_store.lock().await.journal().await? {
            let event: E = EventDescriptor::from_external_representation(&record)?;
            self.tx
                .send(event)
                .map_err(|broadcast::error::SendError(event)| {
                    Error::Generic(format!("SendError {event:?}"))
                })?;
        }
        Ok(())
    }

    // Can I do something here to force a persist to be required before issuing a send?
    // It is not possible to
    async fn emit(&self, event: E) -> Result<()> {
        let mut store = self.event_store.lock().await;
        store.persist(event.clone()).await?;
        self.tx
            .send(event)
            .map_err(|broadcast::error::SendError(event)| {
                Error::Generic(format!("Unable to send {:?} to subscribers", event).to_owned())
            })?;

        Ok(())
    }

    fn subscribe(&self) -> EventBusSubscription<E> {
        EventBusSubscription::new(self.tx.subscribe())
    }
}

struct EventBusSubscription<E> {
    rx: Mutex<Receiver<E>>,
}

impl<E> EventBusSubscription<E>
where
    E: EventDescriptor + Clone,
{
    fn new(rx: Receiver<E>) -> Self {
        Self { rx: Mutex::new(rx) }
    }

    async fn poll(&self) -> Result<E> {
        Ok(self.rx.lock().await.recv().await?)
    }
}

#[derive(Default)]
struct WriteModel {
    author_name_ids: HashMap<String, Vec<AuthorId>>,
    book_title_ids: HashMap<String, Vec<BookId>>,
    author_ids: HashSet<AuthorId>,
    reader_id_by_moniker: HashMap<String, ReaderId>,
    books_read: HashMap<ReaderId, HashSet<BookId>>,
}

impl WriteModel {
    fn apply(&mut self, event: Event) {
        match event {
            Event::BookAdded(id, info) => {
                self.book_title_ids.entry(info.title).or_default().push(id)
            }
            Event::AuthorAdded(id, info) => {
                self.author_name_ids
                    .entry(info.name)
                    .or_default()
                    .push(id.clone());
                self.author_ids.insert(id);
            }
            Event::ReaderAdded(id, info) => {
                self.reader_id_by_moniker.insert(info.unique_moniker, id);
            }
            Event::BookRead(id, info) => {
                self.books_read.entry(id).or_default().insert(info.book_id);
            }
        }
    }
}
