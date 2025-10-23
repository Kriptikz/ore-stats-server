
use std::time::Duration;

use ore_api::{consts::TREASURY_ADDRESS, state::{round_pda, Board, Miner, Round, Treasury}};
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_filter::RpcFilterType};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use steel::{AccountDeserialize, Numeric};

use crate::{app_state::{AppMiner, AppState}, BOARD_ADDRESS};

pub async fn update_data_system(connection: RpcClient, app_state: AppState) {
    tracing::info!("Starting update_data_system");
    tokio::spawn(async move {
        let mut board_snapshot = false;
        loop {
            let treasury = if let Ok(treasury) = connection.get_account_data(&TREASURY_ADDRESS).await {
                if let Ok(treasury) = Treasury::try_from_bytes(&treasury) {
                    treasury.clone()
                } else {
                    tracing::error!("Failed to parse Treasury account");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue
                }
            } else {
                tracing::error!("Failed to load treasury account data");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue
            };

            // update treasury
            let r = app_state.treasury.clone();
            let mut l = r.write().await;
            *l = treasury.into();
            drop(l);

            tokio::time::sleep(Duration::from_secs(1)).await;

            let board = if let Ok(board) = connection.get_account_data(&BOARD_ADDRESS).await {
                if let Ok(board) = Board::try_from_bytes(&board) {
                    board.clone()
                } else {
                    tracing::error!("Failed to parse Board account");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            } else {
                tracing::error!("Failed to load board account data");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            };

            // update board
            let r = app_state.board.clone();
            let mut l = r.write().await;
            *l = board.into();
            drop(l);

            if board.end_slot == u64::MAX {
                tracing::info!("Waiting for first deployment");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            let last_deployable_slot = board.end_slot;
            let current_slot = if let Ok(current_slot) = connection.get_slot().await {
                current_slot
            } else {
                tracing::error!("Failed to get slot from rpc");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };

            let slots_left_in_round = last_deployable_slot as i64 - current_slot as i64;

            println!("Slots left for round: {}", slots_left_in_round);
            tokio::time::sleep(Duration::from_secs(1)).await;

            if slots_left_in_round < 0 {
                if !board_snapshot {
                    let round = if let Ok(round) = connection.get_account_data(&round_pda(board.round_id).0).await {
                        if let Ok(round) = Round::try_from_bytes(&round) {
                            round.clone()
                        } else {
                            tracing::error!("Failed to parse Round account");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    } else {
                        tracing::error!("Failed to load round account data");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue
                    };

                    // update round
                    let r = app_state.round.clone();
                    let mut l = r.write().await;
                    *l = round.into();
                    drop(l);

                    tokio::time::sleep(Duration::from_secs(1)).await;

                    let mut miners: Vec<AppMiner> = vec![];
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
                                let mut miner = *miner;
                                miner.refined_ore = infer_refined_ore(&miner, &treasury);
                                miners.push(miner.clone().into());
                            }
                        }
                    }

                    if miners.len() > 0 {
                        // Update miners
                        let r = app_state.miners.clone();
                        let mut l = r.write().await;
                        *l = miners;
                        drop(l);
                    }
                    board_snapshot = true;
                }
            } else if slots_left_in_round > 0 {
                let sleep_time = slots_left_in_round as u64 * 400;
                println!("Sleeping until round is over in {} ms", sleep_time + 5000);
                tokio::time::sleep(Duration::from_millis(sleep_time)).await;
            } else {
                println!("Sleeping for 5 seconds");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }


        }
    });
}

fn infer_refined_ore(miner: &Miner, treasury: &Treasury) -> u64 {
    let delta = treasury.miner_rewards_factor - miner.rewards_factor;
    if delta < Numeric::ZERO {
        // Defensive: shouldn't happen, but keep behavior sane.
        return miner.refined_ore;
    }
    let accrued = (delta * Numeric::from_u64(miner.rewards_ore)).to_u64();
    miner.refined_ore.saturating_add(accrued)
}

pub fn refinement_level_percent(refined_ore: f64, unclaimed_ore: f64) -> f64 {
    if unclaimed_ore <= 0.0 {
        if refined_ore <= 0.0 {
            -10.0
        } else {
            f64::INFINITY
        }
    } else {
        -10.0 + 100.0 * (refined_ore / unclaimed_ore)
    }
}


