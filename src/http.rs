use anyhow::Result;
use axum::{extract::State, routing::get, Extension, Router};
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::{application::CommandQueryOrchestrator, infrastructure::EventStore};

type Orchestrator<ES> = Arc<CommandQueryOrchestrator<ES>>;
pub struct Api<ES>(Orchestrator<ES>);

impl<ES> Api<ES>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    pub fn new(orchestrator: CommandQueryOrchestrator<ES>) -> Self {
        Self(Arc::new(orchestrator))
    }

    pub async fn start(self, listener: TcpListener) -> Result<()> {
        let Self(orchestrator) = self;
        let routes = routing_configuration().with_state(orchestrator);

        axum::serve(listener, routes).await?;
        Ok(())
    }
}

fn routing_configuration<ES>() -> Router<Orchestrator<ES>>
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    Router::new().route("/", get(root))
}

async fn root<ES>(State(orchestrator): State<Orchestrator<ES>>) -> &'static str
where
    ES: EventStore + Send + Sync + Clone + 'static,
{
    //    orchestrator.issue_query(query)

    "Hi, mom"
}
