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

pub async fn get_rounds(pool: &Pool<Sqlite>, limit: i64, offset: i64, ml: Option<bool>) -> Result<Vec<RoundRow>, sqlx::Error> {
    if let Some(ml) = ml {
        if ml {
            let rounds = sqlx::query_as::<_, RoundRow>(
                r#"
                SELECT * FROM rounds
                WHERE motherlode > 0
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                "#
            )
            .bind(limit)
            .bind(offset)
            .fetch_all(pool)
            .await?;

            return Ok(rounds)
        }
    }

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

pub async fn get_miner_rounds(pool: &Pool<Sqlite>, pubkey: String, limit: i64, offset: i64) -> Result<Vec<RoundRow>, sqlx::Error> {
    let rounds = sqlx::query_as::<_, RoundRow>(
        r#"
        SELECT * FROM rounds r
        WHERE EXISTS (
          SELECT 1 FROM deployments d
          WHERE d.round_id = r.id
            AND d.pubkey   = ?
        )
        ORDER BY r.created_at DESC
        LIMIT ? OFFSET ?
        "#
    )
    .bind(pubkey)
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

#[derive(Serialize, Deserialize, Debug, Clone, sqlx::FromRow)]
pub struct MinerTotalsRow {
    pub pubkey: String,
    pub rounds_played: i64,
    pub rounds_won: i64,                 // NEW
    pub total_sol_deployed: i64,
    pub total_sol_earned: i64,
    pub total_ore_earned: i64,
    pub net_sol_change: i64,
    pub sol_balance_direction: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, sqlx::FromRow)]
pub struct MinerLeaderboardRow {
    pub rank: i64,
    pub pubkey: String,
    pub rounds_played: i64,
    pub rounds_won: i64,
    pub total_sol_deployed: i64,
    pub total_sol_earned: i64,
    pub total_ore_earned: i64,
    pub net_sol_change: i64,
    pub sol_balance_direction: String,
}

pub async fn get_miner_totals_all_time(
    pool: &sqlx::SqlitePool,
    limit: i64,
    offset: i64,
) -> anyhow::Result<Vec<MinerTotalsRow>> {
    let rows = sqlx::query_as::<_, MinerTotalsRow>(r#"
        WITH per_miner_round AS (
          SELECT
            d.pubkey,
            d.round_id,
            SUM(d.amount)      AS total_deployed,
            SUM(d.sol_earned)  AS total_sol_earned,
            SUM(d.ore_earned)  AS total_ore_earned,
            MAX(CASE WHEN d.square_id = r.winning_square THEN 1 ELSE 0 END) AS won_round,
            (SUM(d.sol_earned) - SUM(d.amount)) AS net_sol_round
          FROM deployments d
          JOIN rounds r ON r.id = d.round_id
          GROUP BY d.pubkey, d.round_id
        )
        SELECT
          pubkey,
          COUNT(*)                                  AS rounds_played,
          SUM(won_round)                            AS rounds_won,
          SUM(total_deployed)                       AS total_sol_deployed,
          SUM(total_sol_earned)                     AS total_sol_earned,
          SUM(total_ore_earned)                     AS total_ore_earned,
          SUM(net_sol_round)                        AS net_sol_change,
          CASE
            WHEN SUM(net_sol_round) > 0 THEN 'up'
            WHEN SUM(net_sol_round) < 0 THEN 'down'
            ELSE 'flat'
          END AS sol_balance_direction
        FROM per_miner_round
        GROUP BY pubkey
        HAVING COUNT(*) >= 100
        ORDER BY net_sol_change DESC
        LIMIT ? OFFSET ?;
    "#)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn get_leaderboard_last_60_rounds(
    pool: &sqlx::SqlitePool,
    limit: i64,
    offset: i64,
) -> anyhow::Result<Vec<MinerLeaderboardRow>> {
    let rows = sqlx::query_as::<_, MinerLeaderboardRow>(r#"
        WITH last_60_rounds AS (
          SELECT id
          FROM rounds
          ORDER BY id DESC
          LIMIT 60
        ),
        per_miner_round AS (
          SELECT
            d.pubkey,
            d.round_id,
            SUM(d.amount)      AS total_deployed,
            SUM(d.sol_earned)  AS total_sol_earned,
            SUM(d.ore_earned)  AS total_ore_earned,
            MAX(CASE WHEN d.square_id = r.winning_square THEN 1 ELSE 0 END) AS won_round,
            (SUM(d.sol_earned) - SUM(d.amount)) AS net_sol_round
          FROM deployments d
          JOIN rounds r ON r.id = d.round_id
          WHERE d.round_id IN (SELECT id FROM last_60_rounds)
          GROUP BY d.pubkey, d.round_id
        ),
        miner_aggs AS (
          SELECT
            pubkey,
            COUNT(*)              AS rounds_played,
            SUM(won_round)        AS rounds_won,
            SUM(total_deployed)   AS total_sol_deployed,
            SUM(total_sol_earned) AS total_sol_earned,
            SUM(total_ore_earned) AS total_ore_earned,
            SUM(net_sol_round)    AS net_sol_change
          FROM per_miner_round
          GROUP BY pubkey
        )
        SELECT
          ROW_NUMBER() OVER (ORDER BY net_sol_change DESC) AS rank,
          pubkey,
          rounds_played,
          rounds_won,
          total_sol_deployed,
          total_sol_earned,
          total_ore_earned,
          net_sol_change,
          CASE
            WHEN net_sol_change > 0 THEN 'up'
            WHEN net_sol_change < 0 THEN 'down'
            ELSE 'flat'
          END AS sol_balance_direction
        FROM miner_aggs
        ORDER BY rank
        LIMIT ? OFFSET ?;
    "#)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}


#[derive(Serialize, Deserialize, Debug, Clone, sqlx::FromRow)]
pub struct MinerOreLeaderboardRow {
    pub rank: i64,                // ranked by total_ore_earned DESC
    pub pubkey: String,
    pub rounds_played: i64,
    pub rounds_won: i64,
    pub total_sol_deployed: i64,
    pub total_sol_earned: i64,
    pub total_ore_earned: i64,
    pub net_sol_change: i64,      // still useful context even though we sort by ore
}


pub async fn get_ore_leaderboard_all_time(
    pool: &sqlx::SqlitePool,
    limit: i64,
    offset: i64,
) -> anyhow::Result<Vec<MinerOreLeaderboardRow>> {
    let rows = sqlx::query_as::<_, MinerOreLeaderboardRow>(r#"
        WITH per_miner_round AS (
          SELECT
            d.pubkey,
            d.round_id,
            SUM(d.amount)      AS total_deployed,
            SUM(d.sol_earned)  AS total_sol_earned,
            SUM(d.ore_earned)  AS total_ore_earned,
            MAX(CASE WHEN d.square_id = r.winning_square THEN 1 ELSE 0 END) AS won_round,
            (SUM(d.sol_earned) - SUM(d.amount)) AS net_sol_round
          FROM deployments d
          JOIN rounds r ON r.id = d.round_id
          GROUP BY d.pubkey, d.round_id
        ),
        miner_aggs AS (
          SELECT
            pubkey,
            COUNT(*)                  AS rounds_played,
            SUM(won_round)            AS rounds_won,
            SUM(total_deployed)       AS total_sol_deployed,
            SUM(total_sol_earned)     AS total_sol_earned,
            SUM(total_ore_earned)     AS total_ore_earned,
            SUM(net_sol_round)        AS net_sol_change
          FROM per_miner_round
          GROUP BY pubkey
          HAVING COUNT(*) >= 100
        )
        SELECT
          ROW_NUMBER() OVER (ORDER BY total_ore_earned DESC, total_sol_earned DESC) AS rank,
          pubkey,
          rounds_played,
          rounds_won,
          total_sol_deployed,
          total_sol_earned,
          total_ore_earned,
          net_sol_change
        FROM miner_aggs
        ORDER BY rank
        LIMIT ? OFFSET ?;
    "#)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}



pub async fn get_ore_leaderboard_last_n_rounds(
    pool: &sqlx::SqlitePool,
    n_rounds: i64,
    limit: i64,
    offset: i64,
) -> anyhow::Result<Vec<MinerOreLeaderboardRow>> {
    let rows = sqlx::query_as::<_, MinerOreLeaderboardRow>(r#"
        WITH last_n_rounds AS (
          SELECT id
          FROM rounds
          ORDER BY id DESC
          LIMIT ?
        ),
        per_miner_round AS (
          SELECT
            d.pubkey,
            d.round_id,
            SUM(d.amount)      AS total_deployed,
            SUM(d.sol_earned)  AS total_sol_earned,
            SUM(d.ore_earned)  AS total_ore_earned,
            MAX(CASE WHEN d.square_id = r.winning_square THEN 1 ELSE 0 END) AS won_round,
            (SUM(d.sol_earned) - SUM(d.amount)) AS net_sol_round
          FROM deployments d
          JOIN rounds r ON r.id = d.round_id
          WHERE d.round_id IN (SELECT id FROM last_n_rounds)
          GROUP BY d.pubkey, d.round_id
        ),
        miner_aggs AS (
          SELECT
            pubkey,
            COUNT(*)                  AS rounds_played,
            SUM(won_round)            AS rounds_won,
            SUM(total_deployed)       AS total_sol_deployed,
            SUM(total_sol_earned)     AS total_sol_earned,
            SUM(total_ore_earned)     AS total_ore_earned,
            SUM(net_sol_round)        AS net_sol_change
          FROM per_miner_round
          GROUP BY pubkey
        )
        SELECT
          ROW_NUMBER() OVER (ORDER BY total_ore_earned DESC, total_sol_earned DESC) AS rank,
          pubkey,
          rounds_played,
          rounds_won,
          total_sol_deployed,
          total_sol_earned,
          total_ore_earned,
          net_sol_change
        FROM miner_aggs
        ORDER BY rank
        LIMIT ? OFFSET ?;
    "#)
    .bind(n_rounds.max(1))
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn get_miner_stats(
    pool: &sqlx::SqlitePool,
    pubkey: String,
) -> anyhow::Result<Option<MinerTotalsRow>> {
    let row = sqlx::query_as::<_, MinerTotalsRow>(r#"
        WITH per_miner_round AS (
          SELECT
            d.pubkey,
            d.round_id,
            SUM(d.amount)      AS total_deployed,
            SUM(d.sol_earned)  AS total_sol_earned,
            SUM(d.ore_earned)  AS total_ore_earned,
            MAX(CASE WHEN d.square_id = r.winning_square THEN 1 ELSE 0 END) AS won_round,
            (SUM(d.sol_earned) - SUM(d.amount)) AS net_sol_round
          FROM deployments d
          JOIN rounds r ON r.id = d.round_id
          WHERE d.pubkey = ?
          GROUP BY d.pubkey, d.round_id
        )
        SELECT
          pubkey,
          COUNT(*)                                  AS rounds_played,
          SUM(won_round)                            AS rounds_won,
          SUM(total_deployed)                       AS total_sol_deployed,
          SUM(total_sol_earned)                     AS total_sol_earned,
          SUM(total_ore_earned)                     AS total_ore_earned,
          SUM(net_sol_round)                        AS net_sol_change,
          CASE
            WHEN SUM(net_sol_round) > 0 THEN 'up'
            WHEN SUM(net_sol_round) < 0 THEN 'down'
            ELSE 'flat'
          END AS sol_balance_direction
        FROM per_miner_round
        GROUP BY pubkey;
    "#)
    .bind(pubkey)
    .fetch_optional(pool) // returns Option<T>
    .await?;

    Ok(row)
}

pub async fn get_available_pubkeys(pool: &Pool<Sqlite>, limit: String) -> Result<Vec<String>, sqlx::Error> {
    Ok(vec![])
}

#[derive(Serialize, Deserialize, Debug, Clone, sqlx::FromRow)]
pub struct MinerEarnings24h {
    pub pubkey: String,
    pub window_start: String, // snapshot chosen as ~24h ago
    pub window_end: String,   // latest snapshot
    pub unclaimed_start: i64,
    pub unclaimed_end: i64,
    pub refined_start: i64,
    pub refined_end: i64,
    pub unclaimed_delta: i64,
    pub refined_delta: i64,
    pub pct_earnings: f64, // refined_delta / unclaimed_start * 100
}

pub async fn get_miner_earnings_24h(
    pool: &Pool<Sqlite>,
    pubkey: &str,
) -> Result<Option<MinerEarnings24h>, sqlx::Error> {
    // This SQL:
    //  - gets the latest snapshot for the pubkey
    //  - gets the snapshot closest to exactly now-24h
    //
    // Note: created_at is RFC3339. SQLite's strftime('%s', ...) can parse standard ISO8601/RFC3339 timestamps.
    // If your stored format deviates, you may need to normalize it on insert or cast in the query.
    let row = sqlx::query!(
        r#"
        WITH params AS (
          SELECT ?1 AS pubkey,
                 strftime('%s','now') AS now_s,
                 strftime('%s','now','-24 hours') AS since_s
        ),
        latest AS (
          SELECT id, pubkey, unclaimed_ore, refined_ore, lifetime_sol, lifetime_ore, created_at
          FROM miner_snapshots
          WHERE pubkey = (SELECT pubkey FROM params)
          ORDER BY created_at DESC
          LIMIT 1
        ),
        start AS (
          -- pick the snapshot *closest* to exactly 24h ago
          SELECT id, pubkey, unclaimed_ore, refined_ore, lifetime_sol, lifetime_ore, created_at
          FROM miner_snapshots
          WHERE pubkey = (SELECT pubkey FROM params)
          ORDER BY ABS(strftime('%s', created_at) - (SELECT since_s FROM params))
          LIMIT 1
        )
        SELECT
          l.pubkey                                     AS "pubkey!: String",
          s.created_at                                 AS "start_created_at!: String",
          l.created_at                                 AS "end_created_at!: String",
          s.unclaimed_ore                              AS "unclaimed_start!: i64",
          l.unclaimed_ore                              AS "unclaimed_end!: i64",
          s.refined_ore                                AS "refined_start!: i64",
          l.refined_ore                                AS "refined_end!: i64"
        FROM latest l
        JOIN start  s ON s.pubkey = l.pubkey
        "#
        ,
        pubkey
    )
    .fetch_optional(pool)
    .await?;

    if let Some(r) = row {
        let unclaimed_delta = r.unclaimed_end - r.unclaimed_start;
        let refined_delta   = r.refined_end   - r.refined_start;

        // Define "% earnings" as refined earned over the last 24h divided by
        // unclaimed at the start of the 24h window.
        let denom = if r.unclaimed_start <= 0 { 1.0 } else { r.unclaimed_start as f64 };
        let pct_earnings = (refined_delta as f64 / denom) * 100.0;

        Ok(Some(MinerEarnings24h {
            pubkey: r.pubkey,
            window_start: r.start_created_at,
            window_end: r.end_created_at,
            unclaimed_start: r.unclaimed_start,
            unclaimed_end: r.unclaimed_end,
            refined_start: r.refined_start,
            refined_end: r.refined_end,
            unclaimed_delta,
            refined_delta,
            pct_earnings,
        }))
    } else {
        // No snapshots for this pubkey
        Ok(None)
    }
}
