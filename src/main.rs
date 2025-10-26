use std::{env, str::FromStr, sync::Arc, time::{Duration, Instant}};

use anyhow::{anyhow, bail};
use sqlx::sqlite::SqliteConnectOptions;
use thiserror::Error;
use axum::{body::Body, extract::{Path, Query, State}, http::{Request, Response, StatusCode}, middleware::{self, Next}, routing::get, Json, Router};
use const_crypto::ed25519;
use ore_api::{consts::{BOARD, ROUND, TREASURY_ADDRESS}, state::{round_pda, Board, Miner, Round, Treasury}};
use serde::{Deserialize, Serialize};
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_filter::RpcFilterType};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use steel::{AccountDeserialize, Pubkey};
use tokio::{signal, sync::{Mutex, RwLock}};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::{app_state::{AppBoard, AppMiner, AppRound, AppState, AppTreasury}, database::{get_deployments_by_round, CreateDeployment, DbMinerSnapshot, DbTreasury, MinerLeaderboardRow, MinerOreLeaderboardRow, MinerTotalsRow, RoundRow}, rpc::update_data_system};

/// Program id for const pda derivations
const PROGRAM_ID: [u8; 32] = unsafe { *(&ore_api::id() as *const Pubkey as *const [u8; 32]) };


/// The address of the board account.
pub const BOARD_ADDRESS: Pubkey =
    Pubkey::new_from_array(ed25519::derive_program_address(&[BOARD], &PROGRAM_ID).0);

/// The address of the square account.
pub const ROUND_ADDRESS: Pubkey =
    Pubkey::new_from_array(ed25519::derive_program_address(&[ROUND], &PROGRAM_ID).0);

pub mod app_state;
pub mod rpc;
pub mod database;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().expect("Failed to load env");

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(env_filter)
        .init();

    let db_url = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://data/app.db".to_string());
    if let Some(path) = db_url.strip_prefix("sqlite://") {
        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent).ok();
        }
    }

    let db_connect_ops = SqliteConnectOptions::from_str(&db_url)?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .pragma("cache_size", "-200000") // Set cache to ~200MB (200,000KB)
        .pragma("temp_store", "memory") // Store temporary data in memory
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(15))
        .foreign_keys(true);

    let db_pool = sqlx::sqlite::SqlitePoolOptions::new()
        .min_connections(2)
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(db_connect_ops)
        .await?;


    tracing::info!("Running optimize...");
    sqlx::query("PRAGMA optimize").execute(&db_pool).await?;
    tracing::info!("Optimize complete!");



    tracing::info!("Running migrations...");

    sqlx::migrate!("./migrations").run(&db_pool).await?;

    tracing::info!("Database migrations complete.");
    tracing::info!("Database ready!");

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

    // Sleep between RPC Calls
    tokio::time::sleep(Duration::from_secs(1)).await;

    let board = if let Ok(board) = connection.get_account_data(&BOARD_ADDRESS).await {
        if let Ok(board) = Board::try_from_bytes(&board) {
            board.clone()
        } else {
            bail!("Failed to parse Board account");
        }
    } else {
        bail!("Failed to load board account data");
    };
    tokio::time::sleep(Duration::from_secs(1)).await;

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
        treasury: Arc::new(RwLock::new(treasury.into())),
        board: Arc::new(RwLock::new(board.into())),
        staring_round: board.round_id,
        rounds: Arc::new(RwLock::new(vec![])),
        miners: Arc::new(RwLock::new(miners)),
        db_pool,
    };

    let s = app_state.clone();
    update_data_system(connection, s).await;

    let state = app_state.clone();

    let app = Router::new()
        .route("/", get(root))
        .route("/treasury", get(get_treasury))
        .route("/board", get(get_board))
        .route("/round", get(get_round))
        .route("/miners", get(get_miners))
        .route("/deployments", get(get_deployments))
        .route("/rounds", get(get_rounds))
        .route("/treasuries", get(get_treasuries))
        .route("/miner/{pubkey}", get(get_miner_history))
        .route("/miner/totals", get(get_miner_totals))
        .route("/miner/totals/ore", get(get_miner_totals_ore))
        .route("/leaderboard", get(get_leaderboard))
        .route("/leaderboard/ore", get(get_leaderboard_ore))
        .layer(middleware::from_fn(log_request_time))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await?;

    tracing::debug!("Listening on {}", listener.local_addr()?);

    axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await?;

    Ok(())
}

async fn log_request_time(
    req: Request<Body>,
    next: Next,
) -> Result<Response<Body>, StatusCode> {
    let start_time = Instant::now();
    let method = req.method().to_string();
    let uri = req.uri().to_string();

    let response = next.run(req).await;

    let duration = start_time.elapsed();
    tracing::info!("Request: {} {} - Duration: {:?}", method, uri, duration);

    Ok(response)
}

async fn root() -> &'static str {
    "ORE"
}

#[derive(Debug, Deserialize)]
struct MinersPagination {
    limit: Option<i64>,
    offset: Option<i64>,
    order_by: Option<String>,
}

async fn get_miners(
    State(state): State<AppState>,
    Query(p): Query<MinersPagination>,
) -> Result<Json<Vec<AppMiner>>, AppError> {
    let limit = p.limit.unwrap_or(2500).max(1).min(2500) as usize;
    let offset = p.offset.unwrap_or(0).max(0) as usize;
    let miners = state.miners.clone();
    let reader = miners.read().await;
    let mut miners = reader.clone();
    drop(reader);
    if miners.len() > 0 {
        match p.order_by {
            Some(v) => {
                if v.eq("unclaimed_sol") {
                    miners.sort_by(|a, b| b.rewards_sol.partial_cmp(&a.rewards_sol).unwrap());
                } else if v.eq("unclaimed_ore") {
                    miners.sort_by(|a, b| b.rewards_ore.partial_cmp(&a.rewards_ore).unwrap());
                } else if v.eq("refined_ore") {
                    miners.sort_by(|a, b| b.refined_ore.partial_cmp(&a.refined_ore).unwrap());
                } else if v.eq("total_deployed") {
                    miners.sort_by(|a, b| b.total_deployed.partial_cmp(&a.total_deployed).unwrap());
                } else if v.eq("round_id") {
                    miners.sort_by(|a, b| b.round_id.partial_cmp(&a.round_id).unwrap());
                }
            },
            None => {
                // No ordering
            }
        }
        let start = offset.min(miners.len() - 2);
        let end = start + limit.min(miners.len() - 1 - start);
        return Ok(Json(miners[start..end].to_vec()));
    }
    Ok(Json(miners))
}

async fn get_treasury(
    State(state): State<AppState>,
) -> Result<Json<AppTreasury>, AppError> {
    let r = state.treasury.clone();
    let lock = r.read().await;
    let data = lock.clone();
    Ok(Json(data))
}


async fn get_board(
    State(state): State<AppState>,
) -> Result<Json<AppBoard>, AppError> {
    let r = state.board.clone();
    let lock = r.read().await;
    let data = lock.clone();
    Ok(Json(data))
}

async fn get_round(
    State(state): State<AppState>,
) -> Result<Json<AppRound>, AppError> {
    let r = state.rounds.clone();
    let lock = r.read().await;
    let data = lock.clone();
    drop(lock);
    if let Some(d) = data.last() {
        Ok(Json(d.clone()))
    } else {
        Err(anyhow!("Failed to get last round").into())
    }
}

#[derive(Debug, Deserialize)]
struct RoundsPagination {
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn get_rounds(
    State(state): State<AppState>,
    Query(p): Query<RoundsPagination>,
) -> Result<Json<Vec<RoundRow>>, AppError> {
    let limit = p.limit.unwrap_or(100).max(1).min(2000);
    let offset = p.offset.unwrap_or(0).max(0);
    let rounds = database::get_rounds(&state.db_pool, limit, offset).await?;
    Ok(Json(rounds))
}

async fn get_treasuries(
    State(state): State<AppState>,
    Query(p): Query<RoundsPagination>,
) -> Result<Json<Vec<DbTreasury>>, AppError> {
    let limit = p.limit.unwrap_or(2000).max(1).min(2000);
    let offset = p.offset.unwrap_or(0).max(0);
    let treasuries = database::get_treasuries(&state.db_pool, limit, offset).await?;
    Ok(Json(treasuries))
}

async fn get_miner_history(
    State(state): State<AppState>,
    Path(pubkey): Path<String>,
    Query(p): Query<RoundsPagination>,
) -> Result<Json<Vec<DbMinerSnapshot>>, AppError> {
    let limit = p.limit.unwrap_or(1200).max(1).min(2000);
    let offset = p.offset.unwrap_or(0).max(0);
    let miners_history = database::get_miner_snapshots(&state.db_pool, pubkey, limit, offset).await?;
    Ok(Json(miners_history))
}

#[derive(Debug, Deserialize)]
struct RoundId {
    round_id: u64,
}

async fn get_deployments(
    State(state): State<AppState>,
    Query(p): Query<RoundId>,
) -> Result<Json<Vec<CreateDeployment>>, AppError> {
    let deployments = get_deployments_by_round(&state.db_pool, p.round_id as i64).await?;
    Ok(Json(deployments))
}

#[derive(Debug, Deserialize)]
struct Pagination {
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn get_miner_totals(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Vec<MinerTotalsRow>>, AppError> {
    let limit = p.limit.unwrap_or(100).clamp(1, 2000);
    let offset = p.offset.unwrap_or(0).max(0);
    let rows = database::get_miner_totals_all_time(&state.db_pool, limit, offset).await?;
    Ok(Json(rows))
}

async fn get_leaderboard(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Vec<MinerLeaderboardRow>>, AppError> {
    let limit = p.limit.unwrap_or(100).clamp(1, 2000);
    let offset = p.offset.unwrap_or(0).max(0);
    let rows = database::get_leaderboard_last_60_rounds(&state.db_pool, limit, offset).await?;
    Ok(Json(rows))
}

#[derive(Debug, Deserialize)]
struct OreLeaderboardQuery {
    limit: Option<i64>,
    offset: Option<i64>,
    //rounds: Option<i64>, // if present, use "Last X rounds"; else All Time
}

async fn get_miner_totals_ore(
    State(state): State<AppState>,
    Query(q): Query<OreLeaderboardQuery>,
) -> Result<Json<Vec<MinerOreLeaderboardRow>>, AppError> {
    let limit  = q.limit.unwrap_or(100).clamp(1, 2000);
    let offset = q.offset.unwrap_or(0).max(0);
    let rows =  database::get_ore_leaderboard_all_time(&state.db_pool, limit, offset).await?;
    Ok(Json(rows))
}

async fn get_leaderboard_ore(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Vec<MinerOreLeaderboardRow>>, AppError> {
    let limit = p.limit.unwrap_or(100).clamp(1, 2000);
    let offset = p.offset.unwrap_or(0).max(0);
    let rows = database::get_ore_leaderboard_last_n_rounds(&state.db_pool, 60, limit, offset).await?;
    Ok(Json(rows))
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
