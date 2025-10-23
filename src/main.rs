use std::{env, str::FromStr, time::Duration};

use axum::{extract::{Path, Query, State}, routing::{get, post}, Json, Router};
use serde::{Deserialize, Serialize};
use tokio::signal;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Clone)]
struct AppState {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().expect("Failed to load env");

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(env_filter)
        .init();

    let rpc_url = env::var("RPC_URL").expect("RPC_URL must be set");

    let app_state = AppState {};

    let app = Router::new()
        .route("/", get(root))
        //.route("/miners", get(get_miners))
        // .route("/blocks", get(get_blocks))
        // .route("/market", get(get_market))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await?;

    tracing::debug!("Listening on {}", listener.local_addr()?);

    axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await?;

    Ok(())
}

async fn root() -> &'static str {
    "Hello, World"
}

#[derive(Debug, Deserialize)]
struct Pagination {
    /// Max rows to return (default 50, max 1000)
    limit: Option<i64>,
    /// Rows to skip (default 0)
    offset: Option<i64>,
}

// async fn get_miners(
//     State(state): State<AppState>,
//     Query(p): Query<Pagination>,
// ) -> Result<Json<Vec<AppMiner>>, AppError> {
//     let limit = p.limit.unwrap_or(50).max(1).min(1000);
//     let offset = p.offset.unwrap_or(0).max(0);

//     let miners = list_miners(&state.pool, limit, offset).await?;
//     Ok(Json(miners))
// }

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        signal(SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutting down");
}
