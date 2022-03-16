CREATE TABLE IF NOT EXISTS nbxv1_evts (
  id SERIAL NOT NULL PRIMARY KEY,
  code TEXT NOT NULL,
  type TEXT NOT NULL,
  data JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS nbxv1_settings (
  code TEXT NOT NULL,
  key TEXT NOT NULL,
  data_bytes bytea DEFAULT NULL,
  PRIMARY KEY (code, key)
);

CREATE INDEX IF NOT EXISTS nbxv1_evts_id ON nbxv1_evts (id DESC);
CREATE INDEX IF NOT EXISTS nbxv1_evts_code_id ON nbxv1_evts (code, id DESC);


CREATE OR REPLACE FUNCTION nbxv1_get_keypath(metadata JSONB, idx BIGINT) RETURNS TEXT language SQL IMMUTABLE AS $$
	   SELECT CASE WHEN metadata->>'type' = 'NBXv1-Derivation' 
	   THEN REPLACE(metadata->>'keyPathTemplate', '*', idx::TEXT) 
	   ELSE NULL END
$$;