use std::env;

use anyhow::bail;
use thiserror::Error;
use axum::{extract::State, routing::get, Json, Router};
use const_crypto::ed25519;
use ore_api::{consts::{BOARD, ROUND, TREASURY_ADDRESS}, state::{Board, Miner, Round, Treasury}};
use serde::{Deserialize, Serialize};
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_filter::RpcFilterType};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use steel::{AccountDeserialize, Pubkey};
use tokio::signal;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::app_state::{AppBoard, AppMiner, AppRound, AppState, AppTreasury};

/// Program id for const pda derivations
const PROGRAM_ID: [u8; 32] = unsafe { *(&ore_api::id() as *const Pubkey as *const [u8; 32]) };


/// The address of the board account.
pub const BOARD_ADDRESS: Pubkey =
    Pubkey::new_from_array(ed25519::derive_program_address(&[BOARD], &PROGRAM_ID).0);

/// The address of the square account.
pub const ROUND_ADDRESS: Pubkey =
    Pubkey::new_from_array(ed25519::derive_program_address(&[ROUND], &PROGRAM_ID).0);

pub mod app_state;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().expect("Failed to load env");

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(env_filter)
        .init();

    let rpc_url = env::var("RPC_URL").expect("RPC_URL must be set");
    let prefix = "https://".to_string();
    let connection = RpcClient::new_with_commitment(prefix + &rpc_url, CommitmentConfig { commitment: CommitmentLevel::Confirmed });

    let treasury = if let Ok(treasury) = connection.get_account_data(&TREASURY_ADDRESS).await {
        if let Ok(treasury) = Treasury::try_from_bytes(&treasury) {
            treasury.clone()
        } else {
            bail!("Failed to parse Treasury account");
        }
    } else {
        bail!("Failed to load treasury account data");
    };

    let board = if let Ok(board) = connection.get_account_data(&BOARD_ADDRESS).await {
        if let Ok(board) = Board::try_from_bytes(&board) {
            board.clone()
        } else {
            bail!("Failed to parse Board account");
        }
    } else {
        bail!("Failed to load board account data");
    };

    let round = if let Ok(round) = connection.get_account_data(&ROUND_ADDRESS).await {
        if let Ok(round) = Round::try_from_bytes(&round) {
            round.clone()
        } else {
            bail!("Failed to parse Round account");
        }
    } else {
        bail!("Failed to load round account data");
    };

    let mut miners = vec![];
    if let Ok(miners_data_raw) = connection.get_program_accounts_with_config(
        &ore_api::id(),
        solana_client::rpc_config::RpcProgramAccountsConfig { 
            filters: Some(vec![RpcFilterType::DataSize(size_of::<Miner>() as u64 + 8)]),
            account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                data_slice: None,
                commitment: Some(CommitmentConfig { commitment: CommitmentLevel::Confirmed }),
                min_context_slot: None,
            },
            with_context: None,
            sort_results: None
        } 
    ).await {
        for miner_data in miners_data_raw {
            if let Ok(miner) = Miner::try_from_bytes(&miner_data.1.data) {
                miners.push(miner.clone().into());
            }
        }
    }

    let app_state = AppState {
        treasury: treasury.into(),
        board: board.into(),
        round: round.into(),
        miners,
    };

    let app = Router::new()
        .route("/", get(root))
        .route("/treasury", get(get_treasury))
        .route("/board", get(get_board))
        .route("/round", get(get_round))
        .route("/miners", get(get_miners))
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
    "ORE"
}

#[derive(Debug, Deserialize)]
struct Pagination {
    /// Max rows to return (default 50, max 1000)
    limit: Option<i64>,
    /// Rows to skip (default 0)
    offset: Option<i64>,
}

async fn get_miners(
    State(state): State<AppState>,
    //Query(p): Query<Pagination>,
) -> Result<Json<Vec<AppMiner>>, AppError> {
    // let limit = p.limit.unwrap_or(50).max(1).min(1000);
    // let offset = p.offset.unwrap_or(0).max(0);
    Ok(Json(state.miners))
}

async fn get_treasury(
    State(state): State<AppState>,
) -> Result<Json<AppTreasury>, AppError> {
    Ok(Json(state.treasury))
}


async fn get_board(
    State(state): State<AppState>,
) -> Result<Json<AppBoard>, AppError> {
    Ok(Json(state.board))
}

async fn get_round(
    State(state): State<AppState>,
) -> Result<Json<AppRound>, AppError> {
    Ok(Json(state.round))
}

#[derive(Error, Debug)]
enum AppError {
    #[error("not found")]
    NotFound,
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

impl axum::response::IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        use axum::{http::StatusCode, Json};
        #[derive(Serialize)]
        struct ErrBody { error: String }
        match self {
            AppError::NotFound => (StatusCode::NOT_FOUND, Json(ErrBody { error: "not found".into() })).into_response(),
            other => {
                tracing::error!("internal error: {other:#}");
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrBody { error: "internal server error".into() })).into_response()
            }
        }
    }
}


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
