CREATE INDEX IF NOT EXISTS idx_snapshots_pubkey_created_at_desc
  ON miner_snapshots(pubkey, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_snapshots_created_at
  ON miner_snapshots(created_at);
