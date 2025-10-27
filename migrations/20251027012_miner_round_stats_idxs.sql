CREATE INDEX IF NOT EXISTS idx_mrs_pubkey   ON miner_round_stats(pubkey);
CREATE INDEX IF NOT EXISTS idx_mrs_round    ON miner_round_stats(round_id);
