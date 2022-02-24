CREATE TABLE IF NOT EXISTS wallets (
  id TEXT NOT NULL PRIMARY KEY,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP);

CREATE TABLE IF NOT EXISTS txs (
  code TEXT NOT NULL,
  id TEXT NOT NULL,
  raw BYTEA NOT NULL,
  indexed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, id));

CREATE TABLE IF NOT EXISTS blks (
  code TEXT NOT NULL,
  id TEXT NOT NULL,
  height BIGINT, prev_id TEXT NOT NULL,
  confirmed BOOLEAN DEFAULT 't',
  indexed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, id));
CREATE INDEX IF NOT EXISTS blks_height ON blks (code, height DESC) WHERE 'confirmed' = 't';

CREATE TABLE IF NOT EXISTS txs_blks (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  blk_id TEXT NOT NULL,
  PRIMARY KEY(code, tx_id, blk_id),
  FOREIGN KEY(code, tx_id) REFERENCES txs (code, id) ON DELETE CASCADE,
  FOREIGN KEY(code, blk_id) REFERENCES blks (code, id) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS scriptpubkeys (
  code TEXT NOT NULL,
  id TEXT NOT NULL,
  addr TEXT NOT NULL,
  PRIMARY KEY(code, id)
);

CREATE TABLE IF NOT EXISTS scriptpubkeys_wallets (
  code TEXT NOT NULL,
  scriptpubkey TEXT NOT NULL,
  wallet_id TEXT NOT NULL,
  PRIMARY KEY(code, scriptpubkey, wallet_id),
  FOREIGN KEY (wallet_id) REFERENCES wallets (id) ON DELETE CASCADE,
  FOREIGN KEY (code, scriptpubkey) REFERENCES scriptpubkeys (code, id) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS outs (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  idx INT NOT NULL,
  scriptpubkey TEXT NOT NULL,
  value BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, tx_id, idx),
  FOREIGN KEY (code, tx_id) REFERENCES txs (code, id) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS wallets_outs (
  wallet_id TEXT NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  idx INT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (wallet_id, code, tx_id, idx),
  FOREIGN KEY (code, tx_id, idx) REFERENCES outs (code, tx_id, idx),
  FOREIGN KEY (wallet_id) REFERENCES wallets (id)
  );
CREATE INDEX wallets_outs_outpoints ON wallets_outs (code, (tx_id || '-' || idx));

CREATE TABLE IF NOT EXISTS ins (
  code TEXT NOT NULL,
  input_tx_id TEXT NOT NULL,
  spent_tx_id TEXT NOT NULL,
  spent_idx INT NOT NULL,
  PRIMARY KEY (code, input_tx_id),
  FOREIGN KEY (code, spent_tx_id, spent_idx) REFERENCES outs (code, tx_id, idx) ON DELETE CASCADE,
  FOREIGN KEY (code, input_tx_id) REFERENCES txs (code, id) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS ins_spent_outs ON ins (code, spent_tx_id, spent_idx);

CREATE TABLE IF NOT EXISTS derivations (
  code TEXT NOT NULL,
  wallet_id TEXT NOT NULL REFERENCES wallets (id) ON DELETE CASCADE,
  scheme TEXT NOT NULL,
  PRIMARY KEY (code, wallet_id, scheme)
);

CREATE TABLE IF NOT EXISTS derivation_lines (
  code TEXT NOT NULL,
  wallet_id TEXT NOT NULL,
  scheme TEXT NOT NULL,
  name TEXT NOT NULL,
  keypath_template TEXT NOT NULL,
  next_index INT DEFAULT 0,
  PRIMARY KEY (code, wallet_id, scheme, name),
  FOREIGN KEY (code, wallet_id, scheme) REFERENCES derivations (code, wallet_id, scheme) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS derivation_lines_scriptpubkeys (
  code TEXT NOT NULL,
  wallet_id TEXT NOT NULL,
  scheme TEXT NOT NULL,
  line_name TEXT NOT NULL,
  idx INT NOT NULL,
  keypath TEXT NOT NULL,
  scriptpubkey TEXT NOT NULL,
  used BOOLEAN DEFAULT 'f',
  -- It would be better if used was not part of the primary key, but we get perf boost on COUNT(used) queries by doing so
  PRIMARY KEY (code, wallet_id, scheme, line_name, used, idx),
  FOREIGN KEY (code, scriptpubkey) REFERENCES scriptpubkeys (code, id) ON DELETE CASCADE,
  FOREIGN KEY (code, wallet_id, scheme, line_name) REFERENCES derivation_lines (code, wallet_id, scheme, name) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS derivation_lines_scriptpubkeys_scriptpubkeys ON derivation_lines_scriptpubkeys (code, scriptpubkey, wallet_id);

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
SELECT wo.code, wo.wallet_id, wo.tx_id, wo.idx, o.scriptpubkey, "value", MAX(COALESCE(ob.height, 0)) height, MIN(o.created_at) created_at
FROM wallets_outs wo
INNER JOIN outs o ON wo.code = o.code AND wo.tx_id = o.tx_id AND wo.idx = o.idx
INNER JOIN txs_blks otb ON wo.code = otb.code AND wo.tx_id = otb.tx_id
INNER JOIN blks ob ON wo.code = ob.code AND otb.blk_id = ob.id
LEFT OUTER JOIN ins i ON wo.code = i.code AND wo.tx_id = i.spent_tx_id AND wo.idx = i.spent_idx
LEFT JOIN txs_blks itb ON wo.code = itb.code AND i.input_tx_id = itb.tx_id
LEFT JOIN blks ib ON wo.code = ib.code AND itb.blk_id = ib.id
GROUP BY wo.code, wo.wallet_id, wo.tx_id, wo.idx, o.scriptpubkey, o.value HAVING BOOL_OR(ob.confirmed) = 't' AND (BOOL_OR(ib.confirmed) = 'f' OR BOOL_OR(ib.confirmed) is null);

CREATE OR REPLACE VIEW tracked_outs AS
SELECT wo.code, wo.wallet_id,  wo.tx_id || '-' || wo.idx spent_outpoint, o.scriptpubkey, dl.keypath_template, dls.idx FROM wallets_outs wo 
INNER JOIN outs o ON wo.code = o.code AND wo.tx_id = o.tx_id AND wo.idx = o.idx 
LEFT JOIN derivation_lines_scriptpubkeys dls ON wo.code = dls.code AND o.scriptpubkey = dls.scriptpubkey AND wo.wallet_id = dls.wallet_id 
LEFT JOIN derivation_lines dl ON wo.code = dl.code AND wo.wallet_id = dl.wallet_id AND dls.scheme = dl.scheme AND dls.line_name = dl.name;

CREATE OR REPLACE VIEW tracked_scriptpubkeys AS
SELECT sw.code, dls.wallet_id, sw.scriptpubkey, dl.keypath_template, dl.name as line_name, dls.idx, dls.keypath FROM scriptpubkeys_wallets sw 
LEFT JOIN derivation_lines_scriptpubkeys dls ON sw.code = dls.code AND sw.scriptpubkey = dls.scriptpubkey AND sw.wallet_id = dls.wallet_id 
LEFT JOIN derivation_lines dl ON sw.code = dl.code AND dls.wallet_id = dl.wallet_id AND dls.scheme = dl.scheme AND dls.line_name = dl.name
