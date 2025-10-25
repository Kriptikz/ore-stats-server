use ore_api::state::{Miner, Round, Treasury};
use serde::{Deserialize, Serialize};
use sqlx::{prelude::FromRow, Pool, Sqlite};

use crate::{app_state::AppMiner};

#[derive(Serialize, Deserialize, Debug, Clone, FromRow)]
pub struct CreateMinerSnapshot {
    pub pubkey: String,
    pub unclaimed_ore: i64,
    pub refined_ore: i64,
    pub lifetime_sol: i64,
    pub lifetime_ore: i64,
    pub created_at: String, // RFC3339
}

impl From<AppMiner> for CreateMinerSnapshot {
    fn from(r: AppMiner) -> Self {
        CreateMinerSnapshot {
            pubkey: r.authority,
            unclaimed_ore: r.rewards_ore as i64,
            refined_ore: r.refined_ore as i64,
            lifetime_sol: r.lifetime_rewards_sol as i64,
            lifetime_ore: r.lifetime_rewards_ore as i64,
            created_at: chrono::Utc::now().to_rfc3339(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, FromRow)]
pub struct DbMinerSnapshot {
    pub id: i64,
    pub pubkey: String,
    pub unclaimed_ore: i64,
    pub refined_ore: i64,
    pub lifetime_sol: i64,
    pub lifetime_ore: i64,
    pub created_at: String, // RFC3339
}

#[derive(Serialize, Deserialize, Debug, Clone, FromRow)]
pub struct CreateTreasury {
    pub balance: i64,
    pub motherlode: i64,
    pub total_staked: i64,
    pub total_unclaimed: i64,
    pub total_refined: i64,
    pub created_at: String, // RFC3339
}

impl From<Treasury> for CreateTreasury {
    fn from(r: Treasury) -> Self {
        CreateTreasury {
            balance: r.balance as i64,
            motherlode: r.motherlode as i64,
            total_staked: r.total_staked as i64,
            total_unclaimed: r.total_unclaimed as i64,
            total_refined: r.total_refined as i64,
            created_at: chrono::Utc::now().to_rfc3339(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, FromRow)]
pub struct DbTreasury {
    pub id: i64,
    pub balance: i64,
    pub motherlode: i64,
    pub total_staked: i64,
    pub total_unclaimed: i64,
    pub total_refined: i64,
    pub created_at: String, // RFC3339
}

#[derive(Serialize, Deserialize, Debug, Clone, FromRow)]
pub struct CreateDeployment {
    pub round_id: i64,
    pub pubkey: String,
    pub square_id: i64,
    pub amount: i64,
    pub sol_earned: i64,
    pub ore_earned: i64,
    pub unclaimed_ore: i64,
    pub created_at: String, // RFC3339
}

#[derive(Serialize, Deserialize, Debug, Clone, FromRow)]
pub struct RoundRow {
    pub id: i64,
    pub slot_hash: Vec<u8>,
    pub winning_square: i64,
    pub expires_at: i64,
    pub motherlode: i64,
    pub rent_payer: String,
    pub top_miner: String,
    pub top_miner_reward: i64,
    pub total_deployed: i64,
    pub total_vaulted: i64,
    pub total_winnings: i64,
    pub created_at: String, // RFC3339
}

impl From<Round> for RoundRow {
    fn from(r: Round) -> Self {
        if let Some(rand) = r.rng() {
            RoundRow {
                id: r.id as i64,
                slot_hash: r.slot_hash.to_vec(),
                winning_square: r.winning_square(rand) as i64,
                expires_at: r.expires_at as i64,
                motherlode: r.motherlode as i64,
                rent_payer: r.rent_payer.to_string(),
                top_miner: r.top_miner.to_string(),
                top_miner_reward: r.top_miner_reward as i64,
                total_deployed: r.total_deployed as i64,
                total_vaulted: r.total_vaulted as i64,
                total_winnings: r.total_winnings as i64,
                created_at: chrono::Utc::now().to_rfc3339(),
            }
        } else {
            RoundRow {
                id: r.id as i64,
                slot_hash: r.slot_hash.to_vec(),
                winning_square: 100,
                expires_at: r.expires_at as i64,
                motherlode: r.motherlode as i64,
                rent_payer: r.rent_payer.to_string(),
                top_miner: r.top_miner.to_string(),
                top_miner_reward: r.top_miner_reward as i64,
                total_deployed: r.total_deployed as i64,
                total_vaulted: r.total_vaulted as i64,
                total_winnings: r.total_winnings as i64,
                created_at: chrono::Utc::now().to_rfc3339(),
            }
        }
    }
}

pub async fn insert_treasury(pool: &Pool<Sqlite>, r: &CreateTreasury) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO treasury (
            balance, motherlode, total_staked, total_unclaimed, total_refined, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        "#
    )
    .bind(r.balance)
    .bind(r.motherlode)
    .bind(r.total_staked)
    .bind(r.total_unclaimed)
    .bind(r.total_refined)
    .bind(&r.created_at)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn get_treasuries(pool: &Pool<Sqlite>, limit: i64, offset: i64) -> Result<Vec<DbTreasury>, sqlx::Error> {
    let treasuries = sqlx::query_as::<_, DbTreasury>(
        r#"
        SELECT * FROM treasury
        ORDER BY id DESC
        LIMIT ? OFFSET ?
        "#
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok(treasuries)
}

pub async fn insert_round(pool: &Pool<Sqlite>, r: &RoundRow) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO rounds (
            id, slot_hash, winning_square, expires_at, motherlode, rent_payer, top_miner,
            top_miner_reward, total_deployed, total_vaulted, total_winnings, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            slot_hash        = excluded.slot_hash,
            winning_square   = excluded.winning_square,
            expires_at       = excluded.expires_at,
            motherlode       = excluded.motherlode,
            rent_payer       = excluded.rent_payer,
            top_miner        = excluded.top_miner,
            top_miner_reward = excluded.top_miner_reward,
            total_deployed   = excluded.total_deployed,
            total_vaulted    = excluded.total_vaulted,
            total_winnings   = excluded.total_winnings,
            created_at       = excluded.created_at
        "#
    )
    .bind(r.id)
    .bind(r.slot_hash.as_slice())
    .bind(r.winning_square)
    .bind(r.expires_at)
    .bind(r.motherlode)
    .bind(r.rent_payer.clone())
    .bind(r.top_miner.clone())
    .bind(r.top_miner_reward)
    .bind(r.total_deployed)
    .bind(r.total_vaulted)
    .bind(r.total_winnings)
    .bind(&r.created_at)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn get_rounds(pool: &Pool<Sqlite>, limit: i64, offset: i64) -> Result<Vec<RoundRow>, sqlx::Error> {
    let rounds = sqlx::query_as::<_, RoundRow>(
        r#"
        SELECT * FROM rounds
        ORDER BY id DESC
        LIMIT ? OFFSET ?
        "#
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok(rounds)
}

pub async fn insert_deployment(pool: &Pool<Sqlite>, d: &CreateDeployment) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO deployments (
            round_id, pubkey, square_id, amount, sol_earned, ore_earned, unclaimed_ore, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(round_id, pubkey, square_id) DO UPDATE SET
            amount        = excluded.amount,
            sol_earned    = excluded.sol_earned,
            ore_earned    = excluded.ore_earned,
            unclaimed_ore = excluded.unclaimed_ore,
            created_at    = excluded.created_at
        "#
    )
    .bind(d.round_id)
    .bind(&d.pubkey)
    .bind(d.square_id)
    .bind(d.amount)
    .bind(d.sol_earned)
    .bind(d.ore_earned)
    .bind(d.unclaimed_ore)
    .bind(&d.created_at)
    .execute(pool)
    .await?;

    Ok(())
}


pub async fn insert_deployments(pool: &Pool<Sqlite>, rows: &[CreateDeployment]) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    for d in rows {
        sqlx::query(
            r#"
            INSERT INTO deployments (
                round_id, pubkey, square_id, amount, sol_earned, ore_earned, unclaimed_ore, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            "#
        )
        .bind(d.round_id)
        .bind(&d.pubkey)
        .bind(d.square_id)
        .bind(d.amount)
        .bind(d.sol_earned)
        .bind(d.ore_earned)
        .bind(d.unclaimed_ore)
        .bind(&d.created_at)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}
pub async fn get_deployments_by_round(
    pool: &Pool<Sqlite>,
    round_id: i64,
) -> Result<Vec<CreateDeployment>, sqlx::Error> {
    let deployments = sqlx::query_as::<_, CreateDeployment>(
        r#"
        SELECT
            round_id, pubkey, square_id, amount,
            sol_earned, ore_earned, unclaimed_ore, created_at
        FROM deployments
        WHERE round_id = ?
        ORDER BY ore_earned DESC
        "#
    )
    .bind(round_id)
    .fetch_all(pool)
    .await?;

    Ok(deployments)
}

pub async fn insert_miner_snapshots(pool: &Pool<Sqlite>, rows: &[CreateMinerSnapshot]) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    for d in rows {
        sqlx::query(
            r#"
            INSERT INTO miner_snapshots (
                pubkey, unclaimed_ore, refined_ore, lifetime_sol, lifetime_ore, created_at
            ) VALUES (?, ?, ?, ?, ?, ?);
            "#
        )
        .bind(&d.pubkey)
        .bind(d.unclaimed_ore)
        .bind(d.refined_ore)
        .bind(d.lifetime_sol)
        .bind(d.lifetime_ore)
        .bind(&d.created_at)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

pub async fn get_miner_snapshots(
    pool: &Pool<Sqlite>,
    pubkey: String,
    limit: i64,
    offset: i64,
) -> Result<Vec<DbMinerSnapshot>, sqlx::Error> {
    let miner_data = sqlx::query_as::<_, DbMinerSnapshot>(
        r#"
        SELECT
            id, pubkey, unclaimed_ore, refined_ore, lifetime_sol, lifetime_ore, created_at
        FROM miner_snapshots
        WHERE pubkey = ?
        ORDER BY id DESC
        LIMIT ? OFFSET ?
        "#
    )
    .bind(pubkey)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok(miner_data)
}
