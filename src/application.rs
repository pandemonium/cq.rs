use super::model::*;
use crate::infrastructure::{EventDescriptor, EventStore, UniqueId};
use anyhow::Result;
use std::{
    cell::RefCell,
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
            write_model: Arc::new(RwLock::new(WriteModel::default())),
        }
    }

    async fn start(&self, terminate: TerminationWaiter) -> task::JoinHandle<()> {
        let mut events = self.event_bus.subscribe();
        let write_model = Arc::clone(&self.write_model);

        self.event_bus
            .replay_journal()
            .await
            .expect("a working replay");

        println!("Spawn CommandDispatcher run loop ");
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
    events: RefCell<Option<EventBusSubscription<Event>>>,
}

impl QueryHandler {
    fn new(subscription: EventBusSubscription<Event>) -> Self {
        Self {
            read_model: Arc::new(RwLock::new(ReadModel::default())),
            events: RefCell::new(Some(subscription)),
        }
    }

    fn start(&self, termination: TerminationWaiter) -> task::JoinHandle<()> {
        let read_model = Arc::clone(&self.read_model);
        let mut events = self
            .events
            .take()
            .expect("Must only start QueryHandler once");

        task::spawn(async move {
            loop {
                tokio::select! {
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

    async fn issue<Q>(&self, query: Q) -> Result<Q::Output>
    where
        Q: ReadModelQuery,
    {
        let read_model = self.read_model.read().await;
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
        let command_dispatcher = self.command_dispatcher.start(termination.waiter());
        let query_handler = self.query_handler.start(termination.waiter());

        let waiter = termination.waiter();
        tokio::select! {
            _ = command_dispatcher => {}
            _ = query_handler => {}
            _ = waiter.wait() => {}
        }
    }

    pub async fn issue_query<Q>(&self, query: Q) -> Result<Q::Output>
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
        println!("Replaying journal");
        for event_repr in self.event_store.lock().await.journal().await {
            let event = EventDescriptor::from_external_representation(event_repr)?;
            self.tx.send(event)?;
        }
        Ok(())
    }

    async fn emit(&self, event: E) -> Result<()> {
        let event = self.event_store.lock().await.persist(event).await?;
        self.tx.send(event)?;
        Ok(())
    }

    fn subscribe(&self) -> EventBusSubscription<E> {
        EventBusSubscription::new(self.tx.subscribe())
    }
}

struct EventBusSubscription<E> {
    rx: Receiver<E>,
}

impl<E> EventBusSubscription<E>
where
    E: EventDescriptor + Sync + Send + Clone + 'static,
{
    fn new(rx: Receiver<E>) -> Self {
        Self { rx }
    }

    async fn poll(&mut self) -> Result<E> {
        // So perhaps I cannot own it?
        // Make it internally mutable.
        Ok(self.rx.recv().await?)
    }
}

pub trait ReadModelQuery {
    type Output;

    fn execute(&self, model: &ReadModel) -> Self::Output;
}

pub struct QueryBooksByAuthorId(pub AuthorId);

impl ReadModelQuery for QueryBooksByAuthorId {
    type Output = Result<Vec<Book>>;

    fn execute(&self, model: &ReadModel) -> Self::Output {
        let QueryBooksByAuthorId(author_id) = self;
        if let Some(book_ids) = model.books_by_author_id.get(author_id) {
            Ok(book_ids
                .iter()
                .filter_map(|id| {
                    model
                        .books
                        .get(id)
                        .map(|info| Book(id.clone(), info.clone()))
                })
                .collect::<Vec<_>>())
        } else {
            Ok(vec![])
        }
    }
}

#[derive(Default)]
pub struct ReadModel {
    authors: HashMap<AuthorId, AuthorInfo>,
    books: HashMap<BookId, BookInfo>,
    books_by_author_id: HashMap<AuthorId, Vec<BookId>>,
}

impl ReadModel {
    fn apply(&mut self, event: Event) {
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
