CREATE TABLE IF NOT EXISTS miner_snapshots (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    pubkey        TEXT NOT NULL,
    unclaimed_ore INTEGER NOT NULL,
    refined_ore   INTEGER NOT NULL,
    lifetime_sol  INTEGER NOT NULL,
    lifetime_ore  INTEGER NOT NULL,
    created_at    TEXT    NOT NULL
);
