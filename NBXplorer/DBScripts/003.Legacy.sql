CREATE TABLE IF NOT EXISTS evts (
  id SERIAL NOT NULL PRIMARY KEY,
  code TEXT NOT NULL,
  type TEXT NOT NULL,
  data JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS evts_id ON evts (id DESC);
CREATE INDEX IF NOT EXISTS evts_code_id ON evts (code, id DESC);


CREATE OR REPLACE FUNCTION get_keypath(metadata JSONB, idx BIGINT) RETURNS TEXT language SQL IMMUTABLE AS $$
	   SELECT CASE WHEN metadata->>'type' = 'NBXv1-Derivation' 
	   THEN REPLACE(metadata->>'keyPathTemplate', '*', idx::TEXT) 
	   ELSE NULL END
$$;