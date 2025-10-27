CREATE TABLE IF NOT EXISTS miner_totals (
  pubkey                   TEXT    NOT NULL PRIMARY KEY,
  rounds_played            INTEGER NOT NULL,
  rounds_won               INTEGER NOT NULL,
  total_sol_deployed       INTEGER NOT NULL,
  total_sol_earned         INTEGER NOT NULL,
  total_ore_earned         INTEGER NOT NULL,
  net_sol_change           INTEGER NOT NULL 
);
