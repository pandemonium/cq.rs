# CQRS

Some kind or other of a Hipster system of CQRS (Command Query Responsibility Separation) with the idea of using EventSourcing.

The sole purpose of this project is to improve my Rust skills and thus far it has succeeded.

# The Thing
The project consists of three crates

## Server
Runs the command and query processing, keeps an event journal with a persistent store thats uses fjall. Exposes an HTTP API using axum with a JSON "REST wire protocol."

## api_client
An API client. Exposes a Rust API for all server HTTP resources. Authentication auschemtication.

## cli
Implements a command line tool that interfaces with the server using api_client.

### the reference

    A book management CLI

    Usage: cli --base-url <BASE_URL> <COMMAND>

    Commands:
      add-author
      add-book
      add-reader
      read-book
      list-authors
      list-books
      list-readers
      list-read-books
      help             Print this message or the help of  the given subcommand(s)

    Options:
          --base-url <BASE_URL>  Base URL of the blister  API
      -h, --help                 Print help
