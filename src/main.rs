use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::time::SystemTime;
use std::{collections::HashMap, fmt, sync::Arc, thread::sleep, time::Duration};
use time::OffsetDateTime;
use tokio::{
    sync::{
        broadcast::{self, Receiver, Sender},
        Mutex, RwLock,
    },
    task,
};
use uuid::{uuid, Uuid};

#[derive(Default)]
struct ReadModel {
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

struct CommandQueryOrchestrator<ES> {
    command_dispatcher: CommandDispatcher<ES>,
    query_handler: QueryHandler,
}

impl<ES> CommandQueryOrchestrator<ES>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    fn new(event_bus: EventBus<ES, Event>) -> Self {
        let event_subscription = event_bus.subscribe();
        CommandQueryOrchestrator {
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

    async fn issue_query<Q>(&self, query: Q) -> Result<Q::Output>
    where
        Q: ReadModelQuery,
    {
        self.query_handler.issue(query).await
    }

    // This belongs in the Command Dispatcher which has a WriteModel
    // ReadModel belongs in the Query Handler
    // They both need to subscribe to events emitted.
    async fn post_command(&self, command: Command) {
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

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
struct UniqueId(Uuid);

impl UniqueId {
    fn fresh() -> Self {
        Self(Uuid::new_v4())
    }
}

enum Command {
    AddBook(BookInfo),
    AddAuthor(AuthorInfo),
}

struct AuthorById(AuthorId);

trait ReadModelQuery {
    type Output;

    fn execute(&self, model: &ReadModel) -> Self::Output;
}

struct QueryBooksByAuthorId(AuthorId);

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

impl AggregateRoot for Author {
    type Id = AuthorId;

    fn try_load(stream: AggregateStream) -> Result<Self> {
        if let Event::AuthorAdded(id, info) = stream.peek()? {
            Ok(Author(id, info))
        } else {
            Err(anyhow!("expected an AuthorAdded"))
        }
    }
}

impl AggregateIdentity for AuthorId {
    type Root = Author;

    fn id(&self) -> &UniqueId {
        let AuthorId(id) = self;
        id
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
struct BookId(UniqueId);

impl AggregateRoot for Book {
    type Id = BookId;

    fn try_load(stream: AggregateStream) -> Result<Self> {
        if let Event::BookAdded(id, info) = stream.peek()? {
            Ok(Book(id, info))
        } else {
            Err(anyhow!("expected an BookAdded"))
        }
    }
}

impl AggregateIdentity for BookId {
    type Root = Book;

    fn id(&self) -> &UniqueId {
        let BookId(id) = self;
        id
    }
}

struct AggregateStream(Vec<ExternalRepresentation>);

impl AggregateStream {
    fn peek<E>(&self) -> Result<E>
    where
        E: EventDescriptor,
    {
        Ok(E::from_external_representation(
            self.0.first().ok_or(anyhow!("expected an event"))?,
        )?)
    }
}

#[derive(Debug)]
struct Author(AuthorId, AuthorInfo);

#[derive(Debug)]
struct Book(BookId, BookInfo);

trait EventStore {
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

trait AggregateRoot: Sized {
    type Id: AggregateIdentity;

    fn try_load(stream: AggregateStream) -> Result<Self>;
}

trait AggregateIdentity {
    type Root: AggregateRoot<Id = Self>;

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

    async fn find_by_aggregate_id(
        &self,
        UniqueId(id): UniqueId,
    ) -> Result<Vec<ExternalRepresentation>> {
        Ok(self
            .events
            .iter()
            .filter(|e| e.aggregate_id == id)
            .cloned()
            .collect())
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

    async fn journal(&self) -> impl Iterator<Item = &ExternalRepresentation> {
        self.events.iter()
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
    let orchestrator = CommandQueryOrchestrator::new(event_bus);

    let termination = Termination::new();
    orchestrator.start(&termination).await;

    orchestrator
        .post_command(Command::AddBook(BookInfo {
            isbn: Isbn("978-1-61729-961-2".to_owned()),
            title: "Functional Design and Architecture".to_owned(),

            // CommandDispatcher to validate this against its WriteModel
            // to make sure this Author exists -- otherwise it rejects
            // the Command.
            author: AuthorId(UniqueId::fresh()),
        }))
        .await;

    orchestrator
        .post_command(Command::AddBook(BookInfo {
            isbn: Isbn("978-91-8002498-3".to_owned()),
            title: "Superrika och jämlika".to_owned(),

            // CommandDispatcher to validate this against its WriteModel
            // to make sure this Author exists -- otherwise it rejects
            // the Command.
            author: AuthorId(UniqueId::fresh()),
        }))
        .await;

    // What is the return value of this?
    let books = orchestrator
        .issue_query(QueryBooksByAuthorId(AuthorId(UniqueId(uuid!(
            "ba68afbe-83a7-4a5e-9619-8a32a8967b28"
        )))))
        .await
        .expect("Where is my book?");

    println!("Books: {books:?}");

    sleep(Duration::from_secs(2));

    termination.signal();
}
