CREATE TABLE IF NOT EXISTS wallets (
  wallet_id TEXT NOT NULL PRIMARY KEY,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP);

CREATE TABLE IF NOT EXISTS txs (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  raw BYTEA NOT NULL,
  invalid BOOLEAN NOT NULL DEFAULT 'f',
  indexed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, tx_id));

CREATE TABLE IF NOT EXISTS blks (
  code TEXT NOT NULL,
  blk_id TEXT NOT NULL,
  height BIGINT, prev_id TEXT NOT NULL,
  confirmed BOOLEAN DEFAULT 't',
  indexed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, blk_id));
CREATE INDEX IF NOT EXISTS blks_code_height_idx ON blks (code, height DESC) WHERE 'confirmed' = 't';
CREATE INDEX IF NOT EXISTS blks_code_blk_id_confirmed_height_idx ON blks (code, blk_id, confirmed, height);

CREATE TABLE IF NOT EXISTS txs_blks (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  blk_id TEXT NOT NULL,
  PRIMARY KEY(code, tx_id, blk_id),
  FOREIGN KEY(code, tx_id) REFERENCES txs ON DELETE CASCADE,
  FOREIGN KEY(code, blk_id) REFERENCES blks ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS scripts (
  code TEXT NOT NULL,
  script TEXT NOT NULL,
  addr TEXT NOT NULL,
  PRIMARY KEY(code, script)
);

CREATE TABLE IF NOT EXISTS explicit_scripts_wallets (
  code TEXT NOT NULL,
  script TEXT NOT NULL,
  wallet_id TEXT NOT NULL REFERENCES wallets ON DELETE CASCADE,
  PRIMARY KEY(code, script, wallet_id),
  FOREIGN KEY (code, script) REFERENCES scripts ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS outs (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  idx INT NOT NULL,
  script TEXT NOT NULL,
  value BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, tx_id, idx),
  FOREIGN KEY (code, tx_id) REFERENCES txs ON DELETE CASCADE,
  FOREIGN KEY (code, script) REFERENCES scripts ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS wallets_outs (
  wallet_id TEXT NOT NULL REFERENCES wallets ON DELETE CASCADE,
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  idx INT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (wallet_id, code, tx_id, idx),
  FOREIGN KEY (code, tx_id, idx) REFERENCES outs ON DELETE CASCADE
  );
CREATE INDEX IF NOT EXISTS wallets_outs_code_outpoint_idx ON wallets_outs (code, (tx_id || '-' || idx));

CREATE TABLE IF NOT EXISTS ins (
  code TEXT NOT NULL,
  input_tx_id TEXT NOT NULL,
  spent_tx_id TEXT NOT NULL,
  spent_idx INT NOT NULL,
  PRIMARY KEY (code, input_tx_id),
  FOREIGN KEY (code, spent_tx_id, spent_idx) REFERENCES outs (code, tx_id, idx) ON DELETE CASCADE,
  FOREIGN KEY (code, input_tx_id) REFERENCES txs (code, tx_id) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS ins_code_spentoutpoint_txid_idx ON ins (code, spent_tx_id, spent_idx, input_tx_id);

CREATE TABLE IF NOT EXISTS derivations (
  code TEXT NOT NULL,
  scheme TEXT NOT NULL,
  PRIMARY KEY (code, scheme)
);

CREATE TABLE IF NOT EXISTS derivations_wallets (
  code TEXT NOT NULL,
  scheme TEXT NOT NULL,
  wallet_id TEXT NOT NULL REFERENCES wallets ON DELETE CASCADE,
  PRIMARY KEY (code, scheme, wallet_id),
  FOREIGN KEY (code, scheme) REFERENCES derivations ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS derivation_lines (
  code TEXT NOT NULL,
  scheme TEXT NOT NULL,
  line_name TEXT NOT NULL,
  keypath_template TEXT NOT NULL,
  next_index INT DEFAULT 0,
  PRIMARY KEY (code, scheme, line_name),
  FOREIGN KEY (code, scheme) REFERENCES derivations ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS derivation_lines_scripts (
  code TEXT NOT NULL,
  scheme TEXT NOT NULL,
  line_name TEXT NOT NULL,
  idx INT NOT NULL,
  keypath TEXT NOT NULL,
  script TEXT NOT NULL,
  used BOOLEAN DEFAULT 'f',
  PRIMARY KEY (code, scheme, line_name, idx),
  FOREIGN KEY (code, script) REFERENCES scripts ON DELETE CASCADE,
  FOREIGN KEY (code, scheme, line_name) REFERENCES derivation_lines ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS derivation_code_script_idx ON derivation_lines_scripts (code, script);

CREATE TABLE IF NOT EXISTS evts (
  id SERIAL NOT NULL PRIMARY KEY,
  code TEXT NOT NULL,
  type TEXT NOT NULL,
  data JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS evts_id ON evts (id DESC);
CREATE INDEX IF NOT EXISTS evts_code_id ON evts (code, id DESC);

CREATE OR REPLACE VIEW conf_utxos AS
WITH conf_outs
AS (
  SELECT wo.code, wo.wallet_id, wo.tx_id, wo.idx, ob.blk_id
  FROM wallets_outs wo
  INNER JOIN txs_blks otb USING (code, tx_id)
  INNER JOIN blks ob USING (code, blk_id)
  WHERE ob.confirmed = 't'
)
SELECT wo.code, wo.wallet_id, wo.tx_id, wo.idx, wo.blk_id FROM conf_outs wo
LEFT JOIN ins i ON wo.code = i.code AND wo.tx_id = i.spent_tx_id AND wo.idx = i.spent_idx
LEFT JOIN txs_blks itb ON wo.code = itb.code AND i.input_tx_id = itb.tx_id
LEFT JOIN blks ib ON wo.code = ib.code AND itb.blk_id=ib.blk_id
GROUP BY wo.code, wo.wallet_id, wo.tx_id, wo.idx, wo.blk_id HAVING BOOL_OR(ib.confirmed) = 'f' OR BOOL_OR(ib.confirmed) is NULL;

CREATE OR REPLACE VIEW tracked_scripts AS
SELECT dls.code, dls.script, dw.wallet_id, 'HD' source, dls.scheme, dls.line_name, dls.keypath, dls.idx, dls.used
FROM derivation_lines_scripts dls
INNER JOIN derivations_wallets dw USING (code, scheme)
UNION ALL
SELECT esw.code, esw.script, esw.wallet_id, 'EXPLICIT' source, NULL, NULL, NULL, NULL, NULL
FROM explicit_scripts_wallets esw;

CREATE OR REPLACE FUNCTION get_wallet_conf_utxos(in_code TEXT, in_wallet_id TEXT)
RETURNS TABLE (code TEXT, wallet_id TEXT, tx_id TEXT, idx INTEGER, script TEXT, value BIGINT, scheme TEXT, line_name TEXT, keypath TEXT) AS $$
  SELECT wo.code, wo.wallet_id, wo.tx_id, wo.idx, ts.script, o.value, ts.scheme, ts.line_name, ts.keypath
  FROM conf_utxos wo
  LEFT JOIN outs o USING (code, tx_id, idx)
  LEFT JOIN tracked_scripts ts USING (code, script)
  WHERE wo.code=$1 AND wo.wallet_id=$2;
$$  LANGUAGE SQL STABLE;

CREATE OR REPLACE VIEW tracked_outs AS
SELECT wo.code, wo.tx_id || '-' || wo.idx spent_outpoint, ts.wallet_id, o.script, o.value, ts.scheme, ts.line_name, ts.keypath FROM wallets_outs wo 
INNER JOIN outs o USING (code, tx_id, idx)
LEFT JOIN tracked_scripts ts USING (code, script);

CREATE OR REPLACE VIEW unconf_txs AS
SELECT t.code, t.tx_id, t.raw, t.indexed_at FROM txs t
LEFT JOIN txs_blks tb USING (code, tx_id)
LEFT JOIN blks b USING (code, blk_id)
WHERE t.invalid = 'f'
GROUP BY t.code, t.tx_id, t.raw, t.indexed_at HAVING BOOL_AND(b.confirmed) = 'f' OR BOOL_OR(b.confirmed) is null;