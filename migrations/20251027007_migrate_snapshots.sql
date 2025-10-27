PRAGMA foreign_keys = OFF;

CREATE TABLE IF NOT EXISTS miner_snapshots_new (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    pubkey        TEXT NOT NULL,
    unclaimed_ore INTEGER NOT NULL,
    refined_ore   INTEGER NOT NULL,
    lifetime_sol  INTEGER NOT NULL,
    lifetime_ore  INTEGER NOT NULL,
    created_at    INTEGER NOT NULL
);

INSERT INTO miner_snapshots_new (
    id, pubkey, unclaimed_ore, refined_ore, lifetime_sol, lifetime_ore, created_at
)
SELECT
    id,
    pubkey,
    unclaimed_ore,
    refined_ore,
    lifetime_sol,
    lifetime_ore,
    CAST(strftime('%s', created_at) AS INTEGER)
FROM miner_snapshots;

DROP TABLE miner_snapshots;

ALTER TABLE miner_snapshots_new RENAME TO miner_snapshots;

PRAGMA foreign_keys = ON;

