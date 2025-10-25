-- winner recovery calc: join on (round_id, square_id)
CREATE INDEX IF NOT EXISTS idx_deployments_round_square
  ON deployments(round_id, square_id);

CREATE INDEX IF NOT EXISTS idx_rounds_winning_square
  ON rounds(winning_square);

