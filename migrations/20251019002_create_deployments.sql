CREATE TABLE IF NOT EXISTS deployments (
    round_id      INTEGER NOT NULL,
    pubkey        TEXT    NOT NULL,
    square_id     INTEGER NOT NULL,
    amount        INTEGER NOT NULL,
    sol_earned    INTEGER NOT NULL,
    ore_earned    INTEGER NOT NULL,
    unclaimed_ore INTEGER NOT NULL,
    created_at    TEXT    NOT NULL
);
