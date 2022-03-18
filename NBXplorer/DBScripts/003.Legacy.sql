CREATE TABLE IF NOT EXISTS nbxv1_evts (
  code TEXT NOT NULL,
  id SERIAL NOT NULL,
  type TEXT NOT NULL,
  data JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, id)
);
CREATE TABLE IF NOT EXISTS nbxv1_evts_ids (
  code TEXT NOT NULL PRIMARY KEY,
  curr_id BIGINT
);

CREATE TABLE IF NOT EXISTS nbxv1_settings (
  code TEXT NOT NULL,
  key TEXT NOT NULL,
  data_bytes bytea DEFAULT NULL,
  data_json JSONB DEFAULT NULL,
  PRIMARY KEY (code, key)
);

CREATE TABLE IF NOT EXISTS nbxv1_metadata (
  wallet_id TEXT NOT NULL REFERENCES wallets ON DELETE CASCADE,
  key TEXT NOT NULL,
  data JSONB,
  PRIMARY KEY (wallet_id, key)
);

CREATE INDEX IF NOT EXISTS nbxv1_evts_id ON nbxv1_evts (id DESC);
CREATE INDEX IF NOT EXISTS nbxv1_evts_code_id ON nbxv1_evts (code, id DESC);


CREATE OR REPLACE FUNCTION nbxv1_get_keypath(metadata JSONB, idx BIGINT) RETURNS TEXT language SQL IMMUTABLE AS $$
	   SELECT CASE WHEN metadata->>'type' = 'NBXv1-Derivation' 
	   THEN REPLACE(metadata->>'keyPathTemplate', '*', idx::TEXT) 
	   ELSE NULL END
$$;
