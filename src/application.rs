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
    infrastructure::{EventDescriptor, EventStore, UniqueId},
    model::{
        Author, AuthorId, AuthorInfo, Book, BookId, BookInfo, Command, DomainError, DomainResult,
        Event,
    },
};

#[derive(Clone)]
pub struct TerminationWaiter(Arc<Mutex<Receiver<()>>>);

impl TerminationWaiter {
    fn new(receiver: Receiver<()>) -> Self {
        Self(Arc::new(Mutex::new(receiver)))
    }

    async fn wait(&self) {
        self.0.lock().await.recv().await.expect("wtf")
    }
}

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
                // Can this be transpanted onto a Book aggregate
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
        }
    }
}

struct QueryHandler {
    read_model: Arc<RwLock<ReadModel>>,
    events: Arc<EventBusSubscription<Event>>,
}

impl QueryHandler {
    fn new(subscription: EventBusSubscription<Event>) -> Self {
        Self {
            read_model: Default::default(),
            events: Arc::new(subscription),
        }
    }

    fn start(&self, termination: TerminationWaiter) -> task::JoinHandle<()> {
        let read_model = Arc::clone(&self.read_model);
        let events = Arc::clone(&self.events);

        task::spawn(async move {
            loop {
                tokio::select! {
                    // is it necessary to have this wrapper? It looks better
                    // but causes a Mutex
                    event = events.poll() => {
                        if let Ok(event) = event {
                            read_model.write().await.apply(event)
                        } else {
                            // log(event.error)
                            break
                        }
                    }
                    _ = termination.wait() => { break }
                }
            }
        })
    }

    async fn issue<Q>(&self, query: Q) -> DomainResult<Q::Output>
    where
        Q: ReadModelQuery,
    {
        let read_model = self.read_model.read().await;

        println!("{read_model:?}");

        Ok(query.execute(&read_model))
    }
}

pub struct CommandQueryOrchestrator<ES> {
    command_dispatcher: CommandDispatcher<ES>,
    query_handler: QueryHandler,
}

impl<ES> CommandQueryOrchestrator<ES>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    pub fn new(event_bus: EventBus<ES, Event>) -> Self {
        let event_subscription = event_bus.subscribe();
        CommandQueryOrchestrator {
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

    pub async fn issue_query<Q>(&self, query: Q) -> DomainResult<Q::Output>
    where
        Q: ReadModelQuery,
    {
        self.query_handler.issue(query).await
    }

    // This belongs in the Command Dispatcher which has a WriteModel
    // ReadModel belongs in the Query Handler
    // They both need to subscribe to events emitted.
    pub async fn submit_command(&self, command: Command) {
        self.command_dispatcher.accept(command).await;
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

    async fn replay_journal(&self) -> DomainResult<()> {
        println!("Replaying journal");
        for event_repr in self.event_store.lock().await.journal().await {
            let event: E = EventDescriptor::from_external_representation(event_repr)?;

            println!("Sending {event:?}");
            self.tx
                .send(event.clone())
                .map_err(|_| DomainError::Generic(format!("SendError {event:?}")))?;
        }
        Ok(())
    }

    // Can I do something here to force a persist to be required before issuing a send?
    // It is not possible to
    async fn emit(&self, event: E) -> DomainResult<()> {
        self.event_store.lock().await.persist(event.clone()).await?;
        self.tx.send(event.clone()).map_err(|_e| {
            DomainError::Generic(format!("Unable to send {:?} to subscribers", event).to_owned())
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
    E: EventDescriptor + Sync + Send + Clone + 'static,
{
    fn new(rx: Receiver<E>) -> Self {
        Self { rx: Mutex::new(rx) }
    }

    async fn poll(&self) -> DomainResult<E> {
        Ok(self.rx.lock().await.recv().await?)
    }
}

pub trait ReadModelQuery {
    type Output;

    fn execute(&self, model: &ReadModel) -> Self::Output;
}

pub struct QueryAllBooks;

impl ReadModelQuery for QueryAllBooks {
    type Output = Vec<Book>;

    fn execute(&self, model: &ReadModel) -> Self::Output {
        model
            .books
            .iter()
            .map(|(id, info)| Book(id.clone(), info.clone()))
            .collect()
    }
}

pub struct QueryAllAuthors;

impl ReadModelQuery for QueryAllAuthors {
    type Output = Vec<Author>;

    fn execute(&self, model: &ReadModel) -> Self::Output {
        model
            .authors
            .iter()
            .map(|(id, info)| Author(id.clone(), info.clone()))
            .collect()
    }
}

pub struct QueryBooksByAuthorId(pub AuthorId);

impl ReadModelQuery for QueryBooksByAuthorId {
    type Output = Vec<Book>;

    fn execute(&self, model: &ReadModel) -> Self::Output {
        let QueryBooksByAuthorId(author_id) = self;
        if let Some(book_ids) = model.books_by_author_id.get(author_id) {
            book_ids
                .iter()
                .filter_map(|id| {
                    model
                        .books
                        .get(id)
                        .map(|info| Book(id.clone(), info.clone()))
                })
                .collect::<Vec<_>>()
        } else {
            vec![]
        }
    }
}

#[derive(Debug, Default)]
pub struct ReadModel {
    authors: HashMap<AuthorId, AuthorInfo>,
    books: HashMap<BookId, BookInfo>,
    books_by_author_id: HashMap<AuthorId, Vec<BookId>>,
}

impl ReadModel {
    fn apply(&mut self, event: Event) {
        println!("Apply {event:?}");
        match event {
            Event::BookAdded(id, info) => {
                self.books.insert(id.clone(), info.clone());
                self.books_by_author_id
                    .entry(info.author)
                    .or_default()
                    .push(id);
            }
            Event::AuthorAdded(id, info) => {
                self.authors.insert(id, info);
            }
        }
    }
}

#[derive(Default)]
struct WriteModel {
    author_name_ids: HashMap<String, Vec<AuthorId>>,
    book_title_ids: HashMap<String, Vec<BookId>>,
    author_ids: HashSet<AuthorId>,
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
        }
    }
}

#[derive(Clone)]
pub struct Termination {
    signal: Sender<()>,
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
