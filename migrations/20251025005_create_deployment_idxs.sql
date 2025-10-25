CREATE INDEX IF NOT EXISTS idx_deployments_pubkey_round ON deployments(pubkey, round_id);
CREATE INDEX IF NOT EXISTS idx_deployments_round        ON deployments(round_id);
CREATE INDEX IF NOT EXISTS idx_deployments_pubkey       ON deployments(pubkey);
CREATE INDEX IF NOT EXISTS idx_deployments_prs          ON deployments(pubkey, round_id, sol_earned, amount);
CREATE INDEX IF NOT EXISTS idx_rounds_id                ON rounds(id);
