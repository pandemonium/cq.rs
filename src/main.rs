use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::error::Error as StdError;
use std::{
    collections::HashMap,
    fmt,
    sync::Arc,
    thread::sleep,
    time::{Duration, Instant},
};
use tokio::{
    sync::{
        broadcast::{self, Receiver, Sender},
        Mutex, RwLock,
    },
    task,
};
use uuid::Uuid;

#[derive(Default)]
struct ReadModel {
    authors: HashMap<AuthorId, AuthorInfo>,
    books: HashMap<BookId, BookInfo>,
}

impl ReadModel {
    fn apply(&mut self, _event: Event) {
        todo!()
    }
}

#[derive(Default)]
struct WriteModel {
    author_name_ids: HashMap<String, Vec<AuthorId>>,
    book_title_ids: HashMap<String, Vec<BookId>>,
}

impl WriteModel {
    fn apply(&mut self, event: Event) {
        match event {
            Event::BookAdded(..) => todo!(),
            Event::AuthorAdded(..) => todo!(),
        }
    }
}

#[derive(Clone)]
struct Termination {
    signal: Arc<Sender<()>>,
    wait: Arc<Mutex<Receiver<()>>>,
}

impl Termination {
    fn new() -> Self {
        let (tx, rx) = broadcast::channel(1);
        Self {
            signal: Arc::new(tx),
            wait: Arc::new(Mutex::new(rx)),
        }
    }

    async fn wait(&self) {
        let waiter = Arc::clone(&self.wait);
        let mut waiter = waiter.lock().await;
        waiter.recv().await.expect("msg");
    }

    fn signal(&self) {
        self.signal.send(()).expect("msg");
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

    fn start(&self, terminate: Termination) -> task::JoinHandle<()> {
        let mut events = self.event_bus.subscribe();
        let write_model = Arc::clone(&self.write_model);

        task::spawn(async move {
            loop {
                tokio::select! {
                    event = events.poll() => {
                        write_model.write().await.apply(event.expect("msg"));
                    }
                    _ = terminate.wait() => {
                        break;
                    }
                }
            }
        })
    }

    async fn run(&self) -> Result<()> {
        let mut events = self.event_bus.subscribe();
        loop {
            let event = events.poll().await?;
            self.write_model.write().await.apply(event);
        }
    }

    async fn accept(&self, command: Command) -> bool {
        match command {
            Command::AddBook(info) => {
                let id = BookId(UniqueId::fresh());
                self.event_bus
                    .emit(Event::BookAdded(id, info))
                    .await
                    .expect("emit");
                true
            }
        }
    }
}

struct QueryHandler {
    read_model: Arc<RwLock<ReadModel>>,
    poller: RefCell<Option<EventBusSubscription<Event>>>,
}

impl QueryHandler {
    fn new(poller: EventBusSubscription<Event>) -> Self {
        Self {
            read_model: Arc::new(RwLock::new(ReadModel::default())),
            poller: RefCell::new(Some(poller)),
        }
    }

    fn start(&self, termination: Termination) -> task::JoinHandle<()> {
        let read_model = Arc::clone(&self.read_model);
        let mut poller = self
            .poller
            .take()
            .expect("Must only start QueryHandler once");

        task::spawn(async move {
            loop {
                tokio::select! {
                    event = poller.poll() => {
                        read_model.write().await.apply(event.expect("msg"));
                    }
                    _ = termination.wait() => { break }
                }
            }
        })
    }

    // Can I know something about the return type, given a query?
    fn pose(&self, _query: Query) -> Vec<String> {
        todo!()
    }
}

struct Cqrs<ES> {
    command_dispatcher: CommandDispatcher<ES>,
    query_handler: QueryHandler,
}

impl<ES> Cqrs<ES>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    fn new(event_bus: EventBus<ES, Event>) -> Self {
        let event_poller = event_bus.subscribe();
        Cqrs {
            command_dispatcher: CommandDispatcher::new(event_bus),
            query_handler: QueryHandler::new(event_poller),
        }
    }

    async fn start(&self, termination: Termination) {
        tokio::select! {
            _ = self.command_dispatcher.start(termination.clone()) => {}
            _ = self.query_handler.start(termination.clone()) => {}
            _ = termination.wait() => {}
        }
    }

    async fn pose(&self, query: Query) {
        self.query_handler.pose(query);
    }

    // This belongs in the Command Dispatcher which has a WriteModel
    // ReadModel belongs in the Query Handler
    // They both need to subscribe to events emitted.
    async fn accept(&mut self, command: Command) {
        self.command_dispatcher.accept(command).await;
    }
}

struct EventBus<ES, E> {
    event_store: Mutex<ES>,
    tx: Sender<E>,
}

impl<ES, E> EventBus<ES, E>
where
    ES: EventStore,
    E: EventDescriptor + Sync + Send + Clone + fmt::Debug + 'static,
{
    fn new(event_store: ES) -> Self {
        let (tx, _rx) = broadcast::channel(100);
        Self {
            event_store: Mutex::new(event_store),
            tx,
        }
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

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
struct UniqueId(Uuid);

impl UniqueId {
    fn fresh() -> Self {
        Self(Uuid::new_v4())
    }
}

enum Command {
    AddBook(BookInfo),
}

enum Query {
    Authors(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BookInfo {
    isbn: Isbn,
    title: String,
    author: AuthorId,
}

#[derive(Clone, Debug)]
struct AuthorInfo {
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Isbn(String);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuthorId(UniqueId);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BookId(UniqueId);

struct AggregateStream {
    id: UniqueId,
    event_stream: Vec<ExternalRepresentation>,
}

impl AggregateStream {
    fn new(id: UniqueId, event_stream: Vec<ExternalRepresentation>) -> Self {
        Self { id, event_stream }
    }
}

#[derive(Debug)]
struct Author(AuthorId, AuthorInfo);

#[derive(Debug)]
struct Book(BookId, BookInfo);

impl TryFrom<AggregateStream> for Author {
    type Error = anyhow::Error;

    fn try_from(_value: AggregateStream) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl TryFrom<AggregateStream> for Book {
    type Error = Box<dyn StdError + Send + Sync>;

    fn try_from(_value: AggregateStream) -> Result<Self, Self::Error> {
        todo!()
    }
}

type AggregateError<Id> = <<Id as AggregateId>::Aggregate as TryFrom<AggregateStream>>::Error;

trait EventStore {
    async fn find_by_event_id(&self, id: UniqueId) -> Result<ExternalRepresentation>;
    async fn load_aggregate<Id>(&self, id: Id) -> Result<Id::Aggregate>
    where
        Id: AggregateId,
        AggregateError<Id>: StdError + Send + Sync + 'static;

    // Use internal mutability instead?
    async fn persist<E>(&mut self, event: E) -> Result<E>
    where
        E: EventDescriptor;

    // fn replay?
}

#[derive(Clone, Debug)]
enum Event {
    BookAdded(BookId, BookInfo),
    AuthorAdded(AuthorId, AuthorInfo),
}

impl Event {
    fn name(&self) -> &str {
        match self {
            Event::BookAdded(..) => "book-added",
            Event::AuthorAdded(..) => "author-added",
        }
    }
}

trait EventDescriptor: Sized {
    async fn external_representation(&self) -> Result<ExternalRepresentation>;
    async fn from_external_representation(external: ExternalRepresentation) -> Result<Self>;
}

impl EventDescriptor for Event {
    async fn external_representation(&self) -> Result<ExternalRepresentation> {
        todo!()
    }

    async fn from_external_representation(_external: ExternalRepresentation) -> Result<Self> {
        todo!()
    }
}

#[derive(Clone, Debug)]
struct ExternalRepresentation {
    id: Uuid,
    when: Instant,
    aggregate_id: Uuid,
    what: String,
    data: JsonValue,
}

#[derive(Clone, Debug, Default)]
struct DummyStore {
    events: Vec<ExternalRepresentation>,
}

impl DummyStore {}

trait AggregateId {
    type Aggregate: TryFrom<AggregateStream> + fmt::Debug;
    fn id(&self) -> &UniqueId;
}

impl EventStore for DummyStore {
    async fn find_by_event_id(&self, UniqueId(id): UniqueId) -> Result<ExternalRepresentation> {
        self.events
            .iter()
            .find(|e| e.id == id)
            .ok_or(anyhow!("No such event"))
            .cloned()
    }

    async fn persist<E>(&mut self, event: E) -> Result<E>
    where
        E: EventDescriptor,
    {
        self.events.push(event.external_representation().await?);
        Ok(event)
    }

    async fn load_aggregate<Id>(&self, aggregate_id: Id) -> Result<Id::Aggregate>
    where
        Id: AggregateId,
        AggregateError<Id>: StdError + Send + Sync + 'static,
    {
        let UniqueId(id) = aggregate_id.id();
        let stream = AggregateStream::new(
            *aggregate_id.id(),
            self.events
                .iter()
                .filter(|e| &e.aggregate_id == id)
                .cloned()
                .collect(),
        );

        Ok(stream.try_into()?)
    }
}

impl AggregateId for BookId {
    type Aggregate = Book;

    fn id(&self) -> &UniqueId {
        let BookId(id) = self;
        id
    }
}

#[tokio::main]
async fn main() {
    let store = DummyStore::default();
    let event_bus = EventBus::new(store);
    let cqrs = Cqrs::new(event_bus);

    //    let x = store
    //        .load_aggregate(BookId(UniqueId::fresh()))
    //        .await
    //        .expect("msg");

    cqrs.accept(Command::AddBook(BookInfo {
        isbn: Isbn("978-1-61729-961-2".to_owned()),
        title: "Functional Design and Architecture".to_owned(),
        author: AuthorId(UniqueId::fresh()),
    }))
    .await;

    let termination = Termination::new();
    cqrs.start(termination.clone()).await;

    sleep(Duration::from_secs(2));

    termination.signal();
}
