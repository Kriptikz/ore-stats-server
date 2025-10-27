-- (A) Build miner_round_stats from deployments+rounds
INSERT OR REPLACE INTO miner_round_stats (
  round_id, pubkey, total_sol_deployed, total_sol_earned, total_ore_earned, won_round, net_sol_round
)
SELECT
  d.round_id,
  d.pubkey,
  SUM(d.amount)                                AS total_sol_deployed,
  SUM(d.sol_earned)                            AS total_sol_earned,
  SUM(d.ore_earned)                            AS total_ore_earned,
  MAX(CASE WHEN d.square_id = r.winning_square THEN 1 ELSE 0 END) AS won_round,
  (SUM(d.sol_earned) - SUM(d.amount))          AS net_sol_round
FROM deployments d
JOIN rounds r ON r.id = d.round_id
GROUP BY d.round_id, d.pubkey;

-- (B) Build miner_totals from miner_round_stats
INSERT OR REPLACE INTO miner_totals (
  pubkey, rounds_played, rounds_won, total_sol_deployed, total_sol_earned, total_ore_earned, net_sol_change
)
SELECT
  pubkey,
  COUNT(*)                         AS rounds_played,
  SUM(won_round)                   AS rounds_won,
  SUM(total_sol_deployed)          AS total_sol_deployed,
  SUM(total_sol_earned)            AS total_sol_earned,
  SUM(total_ore_earned)            AS total_ore_earned,
  SUM(net_sol_round)               AS net_sol_change
FROM miner_round_stats
GROUP BY pubkey;

