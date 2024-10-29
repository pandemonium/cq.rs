use anyhow::Result;
use clap::Parser;

use api_client::{model as domain, ApiClient};
use uuid::Uuid;

pub mod model;

#[derive(Parser)]
#[command(name = "blister")]
#[command(about = "A book management CLI")]
struct CliArgs {
    #[arg(long, help = "Base URL of the blister API")]
    base_url: String,

    #[command(subcommand)]
    command: model::Command,
}

struct BookListServiceApi(ApiClient);

impl BookListServiceApi {
    fn new(client: ApiClient) -> Self {
        Self(client)
    }

    async fn dispatch(&self, command: model::Command) -> Result<()> {
        let Self(client) = self;
        match command {
            model::Command::AddAuthor(info) => Ok(client.add_author(info.try_into()?).await?),
            model::Command::AddBook(info) => Ok(client.add_book(info.try_into()?).await?),
            model::Command::AddReader(info) => Ok(client.add_reader(info.try_into()?).await?),
            model::Command::ReadBook(model::BookRead {
                reader_moniker,
                book_id,
            }) => {
                if let Some(reader) = client.get_reader_by_moniker(&reader_moniker).await? {
                    Ok(client
                        .add_read_book(domain::BookRead {
                            reader_id: reader.id,
                            book_id: domain::BookId(Uuid::parse_str(&book_id)?),
                            when: None,
                        })
                        .await?)
                } else {
                    println!("No such reader with moniker {reader_moniker}");
                    Ok(())
                }
            }
            model::Command::ListAuthors => {
                for author in client.get_authors().await? {
                    println!("{}", model::Author::from(author))
                }
                Ok(())
            }
            model::Command::ListBooks => {
                for (index, book) in model::BookWithAuthor::joined(
                    client.get_books().await?,
                    client.get_authors().await?,
                )
                .into_iter()
                .enumerate()
                {
                    println!("{}. {book}", index + 1);
                    println!()
                }
                Ok(())
            }
            model::Command::ListReaders => {
                for reader in client.get_readers().await? {
                    println!("{}", model::Reader::from(reader))
                }
                Ok(())
            }
            model::Command::ListReadBooks { reader_ref } => {
                if let Some(reader_id) = self.resolve_reader_ref(reader_ref).await? {
                    for (index, book) in client
                        .get_books_read(reader_id)
                        .await?
                        .into_iter()
                        .enumerate()
                    {
                        println!("{}. {}", index + 1, model::Book::from(book));
                    }
                }

                Ok(())
            }
        }
    }

    async fn resolve_reader_ref(
        &self,
        reader: model::ReaderRef,
    ) -> Result<Option<domain::ReaderId>> {
        let Self(client) = self;
        match reader {
            model::ReaderRef::ByReaderId(id) => Ok(Some(domain::ReaderId(id))),
            model::ReaderRef::ByUniqueMoniker(moniker) => {
                Ok(client.get_reader_by_moniker(&moniker).await?.map(|x| x.id))
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args = CliArgs::parse();
    let client = ApiClient::new(&args.base_url);
    let api = BookListServiceApi::new(client);
    api.dispatch(args.command)
        .await
        .expect("command dispatch failed");
}
