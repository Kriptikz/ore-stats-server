CREATE TABLE IF NOT EXISTS rounds (
    id                INTEGER PRIMARY KEY,
    slot_hash         BLOB NOT NULL CHECK(length(slot_hash) = 32),
    winning_square    INTEGER NOT NULL,
    expires_at        INTEGER NOT NULL,
    motherlode        INTEGER NOT NULL,
    rent_payer        TEXT NOT NULL,
    top_miner         TEXT NOT NULL,
    top_miner_reward  INTEGER NOT NULL,
    total_deployed    INTEGER NOT NULL,
    total_vaulted     INTEGER NOT NULL,
    total_winnings    INTEGER NOT NULL,
    created_at        TEXT NOT NULL
);
