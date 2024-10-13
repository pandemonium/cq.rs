use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::cell::RefCell;
use std::error::Error as StdError;
use std::fmt::{Debug, Display};
use std::time::SystemTime;
use std::{
    collections::HashMap,
    fmt,
    sync::Arc,
    thread::sleep,
    time::{Duration, Instant},
};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
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
    fn apply(&mut self, event: Event) {
        match event {
            Event::BookAdded(id, info) => {
                self.books.insert(id, info);
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
}

impl WriteModel {
    fn apply(&mut self, event: Event) {
        match event {
            Event::BookAdded(id, info) => {
                self.book_title_ids.entry(info.title).or_default().push(id)
            }
            Event::AuthorAdded(id, info) => {
                self.author_name_ids.entry(info.name).or_default().push(id)
            }
        }
    }
}

#[derive(Clone)]
struct Termination {
    signal: Sender<()>,
}

impl Termination {
    fn new() -> Self {
        let (signal, _rx) = broadcast::channel(1);
        Self { signal }
    }

    fn waiter(&self) -> TerminationWaiter {
        TerminationWaiter::new(self.signal.subscribe())
    }

    fn signal(&self) {
        self.signal.send(()).expect("msg");
    }
}

#[derive(Clone)]
struct TerminationWaiter(Arc<Mutex<Receiver<()>>>);

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
        let event_subscription = event_bus.subscribe();
        Cqrs {
            command_dispatcher: CommandDispatcher::new(event_bus),
            query_handler: QueryHandler::new(event_subscription),
        }
    }

    async fn start(&self, termination: &Termination) {
        let command_dispatcher = self.command_dispatcher.start(termination.waiter());
        let query_handler = self.query_handler.start(termination.waiter());

        let waiter = termination.waiter();
        tokio::select! {
            _ = command_dispatcher => {}
            _ = query_handler => {}
            _ = waiter.wait() => {}
        }
    }

    async fn pose(&self, query: Query) {
        self.query_handler.pose(query);
    }

    // This belongs in the Command Dispatcher which has a WriteModel
    // ReadModel belongs in the Query Handler
    // They both need to subscribe to events emitted.
    async fn accept(&self, command: Command) {
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

    async fn replay_journal(&self) -> Result<()> {
        println!("Replaying journal");
        for event_repr in self.event_store.lock().await.journal().await {
            println!("--> {event_repr}");
            let event = EventDescriptor::from_external_representation(event_repr)?;

            // Emit event without re-persisting it
            self.tx.send(event)?;

            println!("Done.")
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

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
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
    Books,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BookInfo {
    isbn: Isbn,
    title: String,
    author: AuthorId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuthorInfo {
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Isbn(String);

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
struct AuthorId(UniqueId);

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
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

type AggregateParseError<Id> = <<Id as AggregateId>::Aggregate as TryFrom<AggregateStream>>::Error;

trait EventStore {
    async fn find_by_event_id(&self, id: UniqueId) -> Result<ExternalRepresentation>;
    async fn load_aggregate<Id>(&self, id: Id) -> Result<Id::Aggregate>
    where
        Id: AggregateId,
        AggregateParseError<Id>: StdError + Send + Sync + 'static;

    // Use internal mutability instead?
    async fn persist<E>(&mut self, event: E) -> Result<E>
    where
        E: EventDescriptor;

    async fn journal(&self) -> impl Iterator<Item = &ExternalRepresentation>;
}

#[derive(Clone, Debug)]
enum Event {
    BookAdded(BookId, BookInfo),
    AuthorAdded(AuthorId, AuthorInfo),
}

impl Event {
    const BOOK_ADDED: &str = "book-added";
    const AUTHOR_ADDED: &str = "author-added";

    fn name(&self) -> &str {
        match self {
            Event::BookAdded(..) => Self::BOOK_ADDED,
            Event::AuthorAdded(..) => Self::AUTHOR_ADDED,
        }
    }
}

trait EventDescriptor: Sized {
    fn external_representation(
        &self,
        event_id: UniqueId,
        event_time: SystemTime,
    ) -> Result<ExternalRepresentation>;
    fn from_external_representation(external: &ExternalRepresentation) -> Result<Self>;
}

impl EventDescriptor for Event {
    fn external_representation(
        &self,
        UniqueId(id): UniqueId,
        when: SystemTime,
    ) -> Result<ExternalRepresentation> {
        match self {
            Event::BookAdded(BookId(UniqueId(aggregate_id)), info) => Ok(ExternalRepresentation {
                id,
                when,
                aggregate_id: *aggregate_id,
                what: self.name().to_owned(),
                data: serde_json::to_value(info)?,
            }),
            Event::AuthorAdded(AuthorId(UniqueId(aggregate_id)), info) => {
                Ok(ExternalRepresentation {
                    id,
                    when,
                    aggregate_id: *aggregate_id,
                    what: self.name().to_owned(),
                    data: serde_json::to_value(info)?,
                })
            }
        }
    }

    fn from_external_representation(
        ExternalRepresentation {
            aggregate_id,
            what,
            data,
            ..
        }: &ExternalRepresentation,
    ) -> Result<Self> {
        match what.as_str() {
            Event::AUTHOR_ADDED => Ok(Event::AuthorAdded(
                AuthorId(UniqueId(*aggregate_id)),
                serde_json::from_value(data.clone())?,
            )),
            Event::BOOK_ADDED => Ok(Event::BookAdded(
                BookId(UniqueId(*aggregate_id)),
                serde_json::from_value(data.clone())?,
            )),
            otherwise => Err(anyhow!("Unknown event-type `{}`", otherwise)),
        }
    }
}

#[derive(Clone, Debug)]
struct ExternalRepresentation {
    id: Uuid,
    when: SystemTime,
    aggregate_id: Uuid,
    what: String,
    data: JsonValue,
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

#[derive(Clone, Debug, Default)]
struct DummyStore {
    events: Vec<ExternalRepresentation>,
}

impl DummyStore {}

trait AggregateId {
    type Aggregate: TryFrom<AggregateStream>;
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
        let event_id = UniqueId::fresh();

        // does this take place here?
        // Is this the right data type?
        let timestamp = SystemTime::now();

        let event_rep = event.external_representation(event_id, timestamp)?;
        println!("{}", event_rep);

        self.events.push(event_rep);
        Ok(event)
    }

    async fn load_aggregate<Id>(&self, aggregate_id: Id) -> Result<Id::Aggregate>
    where
        Id: AggregateId,
        AggregateParseError<Id>: StdError + Send + Sync + 'static,
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

    async fn journal(&self) -> impl Iterator<Item = &ExternalRepresentation> {
        self.events.iter()
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
    let cqrs = Cqrs::new(event_bus);

    let termination = Termination::new();
    cqrs.start(&termination).await;

    cqrs.accept(Command::AddBook(BookInfo {
        isbn: Isbn("978-1-61729-961-2".to_owned()),
        title: "Functional Design and Architecture".to_owned(),

        // CommandDispatcher to validate this against its WriteModel
        // to make sure this Author exists -- otherwise it rejects
        // the Command.
        author: AuthorId(UniqueId::fresh()),
    }))
    .await;

    cqrs.accept(Command::AddBook(BookInfo {
        isbn: Isbn("978-91-8002498-3".to_owned()),
        title: "Superrika och jämlika".to_owned(),

        // CommandDispatcher to validate this against its WriteModel
        // to make sure this Author exists -- otherwise it rejects
        // the Command.
        author: AuthorId(UniqueId::fresh()),
    }))
    .await;

    // What is the return value of this?
    cqrs.pose(Query::Books).await;

    sleep(Duration::from_secs(2));

    termination.signal();
}
