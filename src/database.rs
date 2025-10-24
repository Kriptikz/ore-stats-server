use ore_api::state::{Miner, Round};
use serde::{Deserialize, Serialize};
use sqlx::{prelude::FromRow, Pool, Sqlite};

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

#[derive(Debug, Clone)]
pub struct RoundRow {
    pub id: i64,
    pub slot_hash: [u8; 32],
    pub expires_at: i64,
    pub motherlode: i64,
    pub rent_payer: [u8; 32],
    pub top_miner: [u8; 32],
    pub top_miner_reward: i64,
    pub total_deployed: i64,
    pub total_vaulted: i64,
    pub total_winnings: i64,
    pub created_at: String, // RFC3339
}

impl From<Round> for RoundRow {
    fn from(r: Round) -> Self {
        RoundRow {
            id: r.id as i64,
            slot_hash: r.slot_hash,
            expires_at: r.expires_at as i64,
            motherlode: r.motherlode as i64,
            rent_payer: r.rent_payer.to_bytes(),
            top_miner: r.top_miner.to_bytes(),
            top_miner_reward: r.top_miner_reward as i64,
            total_deployed: r.total_deployed as i64,
            total_vaulted: r.total_vaulted as i64,
            total_winnings: r.total_winnings as i64,
            created_at: chrono::Utc::now().to_rfc3339(),
        }
    }
}


pub async fn insert_round(pool: &Pool<Sqlite>, r: &RoundRow) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO rounds (
            id, slot_hash, expires_at, motherlode, rent_payer, top_miner,
            top_miner_reward, total_deployed, total_vaulted, total_winnings, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            slot_hash        = excluded.slot_hash,
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
    .bind(r.expires_at)
    .bind(r.motherlode)
    .bind(r.rent_payer.as_slice())
    .bind(r.top_miner.as_slice())
    .bind(r.top_miner_reward)
    .bind(r.total_deployed)
    .bind(r.total_vaulted)
    .bind(r.total_winnings)
    .bind(&r.created_at)
    .execute(pool)
    .await?;

    Ok(())
}

// pub async fn get_round(pool: &Pool<Sqlite>, round_id: u64) -> Result<RoundRow, sqlx::Error> {
//     let round = sqlx::query_as::<_, RoundRow>(
//         r#"
//         SELECT * FROM rounds
//         WHERE id = ?
//         ORDER BY id DESC
//         "#
//     )
//     .bind(round_id)
//     .execute(pool)
//     .await?;

//     Ok(round)
// }

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
        ORDER BY ore_earned ASC
        "#
    )
    .bind(round_id)
    .fetch_all(pool)
    .await?;

    Ok(deployments)
}

