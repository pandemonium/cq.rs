use api_client::{model as domain, BlockingApiClient};
use cursive::{
    event::Key,
    menu,
    view::{Nameable, Resizable, Scrollable},
    views::{Dialog, LinearLayout, SelectView, TextView},
};
use std::{thread, time::Duration};

// It does not have to own cursive.
// run can create cursive, call view on it, then run.
#[derive(Clone)]
struct UserInterface {
    api: BlockingApiClient,
}

enum ListItem {
    Book(domain::Book),
    Author(domain::Author),
}

impl UserInterface {
    fn new(api: BlockingApiClient) -> Self {
        Self { api }
    }

    fn render(self, siv: &mut cursive::Cursive) {
        siv.with_theme(|t| t.shadow = false);

        let add_menu = menu::Tree::new()
            .leaf("Author...", |s| {
                s.add_layer(Dialog::info("New author"));
            })
            .leaf("Book...", |s| {
                s.add_layer(Dialog::info("New Book"));
            })
            .leaf("Reader...", |s| {
                s.add_layer(Dialog::info("New Reader"));
            });

        siv.menubar()
            .add_leaf("Books", {
                let ui = self.clone();
                move |siv| ui.show_books(siv)
            })
            .add_leaf("Authors", {
                let ui = self.clone();
                move |siv| ui.show_authors(siv)
            })
            .add_subtree("Add", add_menu);

        siv.set_autohide_menu(false);

        let list = SelectView::<ListItem>::new()
            .with_name("list")
            .scrollable()
            .full_screen();

        let mut dashboard = LinearLayout::vertical();
        dashboard.add_child(list);
        dashboard.add_child(TextView::new("Q - exit. Esc menubar.").full_width());

        siv.add_fullscreen_layer(dashboard);

        siv.add_global_callback(Key::Esc, |s| s.select_menubar());
        siv.add_global_callback('q', |s| s.quit());
    }

    fn show_books(&self, siv: &mut cursive::Cursive) {
        if let Some(mut view) = siv.find_name::<SelectView<ListItem>>("list") {
            view.clear();
            for book in self.fetch_books() {
                view.add_item(book.info.title.clone(), ListItem::Book(book));
            }
        }
    }

    fn fetch_books(&self) -> Vec<domain::Book> {
        self.api.get_books().expect("books")
    }

    fn show_authors(&self, siv: &mut cursive::Cursive) {
        if let Some(mut view) = siv.find_name::<SelectView<ListItem>>("list") {
            view.clear();
            for author in self.fetch_authors() {
                view.add_item(author.info.name.clone(), ListItem::Author(author));
            }
        }
    }

    fn fetch_authors(&self) -> Vec<domain::Author> {
        self.api.get_authors().expect("authors")
    }

    fn start(self) {
        let mut siv = cursive::default();
        self.render(&mut siv);
        siv.run();
    }
}

fn main() {
    let api_client = BlockingApiClient::new("http://macaroni.local:3000");
    UserInterface::new(api_client).start()
    //    let authors = api_client.get_authors();
    //    println!("{:?}", authors);
}
