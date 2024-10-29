use anyhow::{Error as AnyhowError, Result};
use clap::{Parser, Subcommand};
use std::{collections::HashMap, fmt};

use api_client::{model, ApiClient};

#[derive(Parser)]
#[command(name = "blister")]
#[command(about = "A book management CLI")]
struct Cli {
    #[arg(long, help = "Base URL of the blister API")]
    base_url: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    AddAuthor(AuthorInfo),
    AddBook(BookInfo),
    AddReader,
    AddReadBook,
    ListAuthors,
    ListBooks,
}

#[derive(Parser)]
struct BookInfo {
    #[arg(long, help = "Title of the book")]
    title: String,

    #[arg(long, help = "ISBN of the book")]
    isbn: String,

    #[arg(long, help = "ID of the author")]
    author_id: String,
}

impl TryFrom<BookInfo> for model::BookInfo {
    type Error = AnyhowError;

    fn try_from(
        BookInfo {
            title,
            isbn,
            author_id,
        }: BookInfo,
    ) -> Result<Self> {
        Ok(Self {
            isbn,
            title,
            author: model::AuthorId(author_id.parse()?),
        })
    }
}

#[derive(Parser)]
struct AuthorInfo {
    #[arg(long, help = "Name of the author")]
    name: String,
}

impl TryFrom<AuthorInfo> for model::AuthorInfo {
    type Error = AnyhowError;

    fn try_from(AuthorInfo { name }: AuthorInfo) -> Result<Self> {
        Ok(Self { name })
    }
}

#[derive(Parser)]
struct ReaderInfo {
    name: String,
    unique_moniker: String,
}

#[derive(Clone)]
struct Author(model::Author);

impl fmt::Display for Author {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(model::Author {
            id: model::AuthorId(id),
            info: model::AuthorInfo { name },
        }) = self;
        write!(f, "[{id}]\t{name}")
    }
}

impl From<model::Author> for Author {
    fn from(value: model::Author) -> Self {
        Self(value)
    }
}

#[derive(Clone)]
struct Book(model::Book);

impl fmt::Display for Book {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(model::Book {
            id: model::BookId(id),
            info:
                model::BookInfo {
                    isbn,
                    title,
                    author,
                },
        }) = self;

        // I would like the author name here.
        // Display this multilined (can this be configurable?)
        write!(f, "[{id}] {title}")
    }
}

impl From<model::Book> for Book {
    fn from(value: model::Book) -> Self {
        Self(value)
    }
}

struct BookWithAuthor(Book, Author);

impl BookWithAuthor {
    fn joined(books: Vec<model::Book>, authors: Vec<model::Author>) -> Vec<BookWithAuthor> {
        let author_by_id = authors
            .iter()
            .map(|x| (&x.id, x))
            .collect::<HashMap<_, _>>();

        books
            .into_iter()
            .filter_map(|x| {
                author_by_id
                    .get(&x.info.author)
                    .map(|&author| Self(x.into(), author.clone().into()))
            })
            .collect()
    }
}

impl fmt::Display for BookWithAuthor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(
            Book(model::Book {
                id: model::BookId(book_id),
                info: model::BookInfo { isbn, title, .. },
            }),
            Author(model::Author {
                id: model::AuthorId(author_id),
                info: model::AuthorInfo { name: author_name },
            }),
        ) = self;
        writeln!(f, "{title} [{isbn}]")?;
        writeln!(f, "{author_name} [Author ID {author_id}]")?;
        write!(f, "[Book ID {book_id}]")
    }
}

struct BookListApi(ApiClient);

impl BookListApi {
    fn new(client: ApiClient) -> Self {
        Self(client)
    }

    async fn dispatch(&self, command: Command) -> Result<()> {
        let Self(client) = self;
        match command {
            Command::AddAuthor(info) => Ok(client.put_author(info.try_into()?).await?),
            Command::AddBook(info) => Ok(client.put_book(info.try_into()?).await?),
            Command::AddReader => todo!(),
            Command::AddReadBook => todo!(),
            Command::ListAuthors => {
                for author in client.get_authors().await? {
                    println!("{}", Author::from(author))
                }
                Ok(())
            }
            Command::ListBooks => {
                for (index, book) in
                    BookWithAuthor::joined(client.get_books().await?, client.get_authors().await?)
                        .into_iter()
                        .enumerate()
                {
                    println!("{}. {book}", index + 1);
                    println!()
                }
                Ok(())
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let client = ApiClient::new(&args.base_url);
    let api = BookListApi::new(client);
    api.dispatch(args.command).await.expect("a book to be put");
}
