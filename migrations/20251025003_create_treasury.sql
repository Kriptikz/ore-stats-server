CREATE TABLE IF NOT EXISTS treasury (
    id              INTEGER NOT NULL,
    balance         INTEGER NOT NULL,
    motherlode      INTEGER NOT NULL,
    total_staked    INTEGER NOT NULL,
    total_unclaimed INTEGER NOT NULL,
    total_refined   INTEGER NOT NULL,
    created_at      TEXT    NOT NULL
);
