CREATE TABLE IF NOT EXISTS miner_round_stats (
  round_id                INTEGER NOT NULL,
  pubkey                  TEXT    NOT NULL,
  total_sol_deployed      INTEGER NOT NULL,
  total_sol_earned        INTEGER NOT NULL,
  total_ore_earned        INTEGER NOT NULL,
  won_round               INTEGER NOT NULL,
  net_sol_round           INTEGER NOT NULL,
  PRIMARY KEY (round_id, pubkey)
);
