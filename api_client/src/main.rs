use api_client::{model, ApiClient};

#[tokio::main]
async fn main() {
    let client = ApiClient::new("http://dsky.local:3000");

    let books = client.get_books().await.expect("some books");
    println!("Books: {books:?}");

    let authors = client.get_authors().await.expect("some authors");
    println!("Authors: {authors:?}");

    for model::Book { id, info } in books {
        let author = client.get_author_by_book(id).await.expect("book's author");
        println!(
            "{} by {}",
            info.title,
            author.expect("existing author").info.name
        );
    }

    for model::Author { id, info } in authors {
        let books = client.get_books_by_author(id).await.expect("some books");
        println!("Books by {}: {books:?}", info.name);
    }

    for model::SearchResultItem { uri, hit } in client.search("Bo").await.expect("msg") {
        match hit {
            model::SearchHit::BookTitle { title, .. } => println!("Title '{title}, at: {uri}'"),
            model::SearchHit::BookIsbn { isbn, .. } => println!("ISBN '{isbn}, at: {uri}'"),
            model::SearchHit::Author { name, .. } => println!("Author '{name}, at: {uri}'"),
        }
    }

    //    client
    //        .put_author(model::AuthorInfo {
    //            name: "Magnus Ranstorp".to_owned(),
    //        })
    //        .await
    //        .expect("msg");

    //    client
    //        .put_book(model::BookInfo {
    //            isbn: "978-91-8002504-1".to_owned(),
    //            title: "Hamas Terror Innifrån".to_owned(),
    //            author: model::AuthorId(uuid!("116dedaf-b5ba-4cb8-8b5a-1c5f08098f22")),
    //        })
    //        .await
    //        .expect("msg");
}
