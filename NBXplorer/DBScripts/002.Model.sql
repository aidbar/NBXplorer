CREATE TABLE IF NOT EXISTS txs (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  raw BYTEA NOT NULL,
  mempool BOOLEAN NOT NULL,
  indexed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, tx_id));

  -- In theory, we could check whether a tx isn't in a confirmed block.
  -- The problem is that it would need a LEFT JOIN ... block IS NULL, which
  -- would require a full scan over the whole txs table.
  -- This field is automatically updated via triggers on blks and txs_blks
  CREATE INDEX IF NOT EXISTS txs_code_mempool ON txs (code, mempool) INCLUDE (tx_id) WHERE mempool='t';


CREATE TABLE IF NOT EXISTS blks (
  code TEXT NOT NULL,
  blk_id TEXT NOT NULL,
  height BIGINT, prev_id TEXT NOT NULL,
  confirmed BOOLEAN DEFAULT 't',
  indexed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, blk_id));
CREATE INDEX IF NOT EXISTS blks_code_height_idx ON blks (code, height DESC) WHERE confirmed = 't';
CREATE INDEX IF NOT EXISTS blks_code_blk_id_confirmed_height_idx ON blks (code, blk_id, confirmed, height);

-- If the blks confirmation state change, update the mempool flag of the txs
CREATE OR REPLACE FUNCTION set_tx_mempool()
  RETURNS TRIGGER 
  LANGUAGE PLPGSQL
AS $$
BEGIN
	UPDATE txs t SET mempool= NOT NEW.confirmed
	FROM (SELECT tb.tx_id FROM txs_blks tb WHERE code=NEW.code AND blk_id=NEW.blk_id) AS q
	WHERE t.code=NEW.code AND t.tx_id=q.tx_id;
	RETURN NEW;
END;
$$;
CREATE TRIGGER blks_mempool AFTER UPDATE ON blks FOR EACH ROW EXECUTE PROCEDURE set_tx_mempool();
--------------------


-- If the a txs get added to a block, update the mempool flag of the txs
CREATE TABLE IF NOT EXISTS txs_blks (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  blk_id TEXT NOT NULL,
  PRIMARY KEY(code, tx_id, blk_id),
  FOREIGN KEY(code, tx_id) REFERENCES txs ON DELETE CASCADE,
  FOREIGN KEY(code, blk_id) REFERENCES blks ON DELETE CASCADE);

CREATE OR REPLACE FUNCTION unset_tx_mempool()
  RETURNS TRIGGER 
  LANGUAGE PLPGSQL
AS $$
BEGIN
	UPDATE txs SET mempool=(SELECT NOT b.confirmed FROM blks b WHERE b.blk_id=NEW.blk_id)
	WHERE code=NEW.code AND tx_id=NEW.tx_id;
	RETURN NEW;
END;
$$;
CREATE TRIGGER txs_blks_mempool AFTER INSERT ON txs_blks FOR EACH ROW EXECUTE PROCEDURE unset_tx_mempool();


CREATE TABLE IF NOT EXISTS scripts (
  code TEXT NOT NULL,
  script TEXT NOT NULL,
  addr TEXT NOT NULL,
  used BOOLEAN NOT NULL DEFAULT 'f'
  /* PRIMARY KEY(code, script) See index below */
);
CREATE UNIQUE INDEX scripts_pkey ON scripts (code, script) INCLUDE (addr, used);
ALTER TABLE scripts ADD CONSTRAINT scripts_pkey PRIMARY KEY USING INDEX scripts_pkey;

CREATE TABLE IF NOT EXISTS outs (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  idx INT NOT NULL,
  script TEXT NOT NULL,
  value BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  /* PRIMARY KEY (code, tx_id, idx) (enforced with index), */
  FOREIGN KEY (code, tx_id) REFERENCES txs ON DELETE CASCADE,
  FOREIGN KEY (code, script) REFERENCES scripts ON DELETE CASCADE);
CREATE UNIQUE INDEX outs_pkey ON outs (code, tx_id, idx) INCLUDE (script, value);
ALTER TABLE outs ADD CONSTRAINT outs_pkey PRIMARY KEY USING INDEX outs_pkey;

CREATE OR REPLACE FUNCTION to_outpoint(in_tx_id TEXT, in_idx INT)
RETURNS TEXT AS $$
  SELECT $1 || '-' || $2
$$  LANGUAGE SQL IMMUTABLE;

-- Can we remove this? We need this to make a SELECT... FROM.. IN () request including both tx_id and idx
-- maybe there is another solution though.
CREATE INDEX IF NOT EXISTS outs_code_outpoint_idx ON outs (code, to_outpoint(tx_id, idx));


CREATE TABLE IF NOT EXISTS ins (
  code TEXT NOT NULL,
  input_tx_id TEXT NOT NULL,
  spent_tx_id TEXT NOT NULL,
  spent_idx INT NOT NULL,
  PRIMARY KEY (code, input_tx_id),
  FOREIGN KEY (code, spent_tx_id, spent_idx) REFERENCES outs (code, tx_id, idx) ON DELETE CASCADE,
  FOREIGN KEY (code, input_tx_id) REFERENCES txs (code, tx_id) ON DELETE CASCADE,
  FOREIGN KEY (code, spent_tx_id) REFERENCES txs (code, tx_id) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS ins_code_spentoutpoint_txid_idx ON ins (code, spent_tx_id, spent_idx, input_tx_id);

CREATE TABLE IF NOT EXISTS descriptors (
  code TEXT NOT NULL,
  descriptor TEXT NOT NULL,
  next_index INT DEFAULT 0,
  PRIMARY KEY (code, descriptor)
);

CREATE TABLE IF NOT EXISTS descriptors_scripts (
  code TEXT NOT NULL,
  descriptor TEXT NOT NULL,
  idx TEXT NOT NULL,
  script TEXT NOT NULL,
  keypath TEXT NOT NULL,
  /* PRIMARY KEY (code, descriptor, idx) , Enforced via index */
  FOREIGN KEY (code, script) REFERENCES scripts ON DELETE CASCADE
);
CREATE UNIQUE INDEX descriptors_scripts_pkey ON descriptors_scripts (code, descriptor, idx) INCLUDE (script);
ALTER TABLE descriptors_scripts ADD CONSTRAINT descriptors_scripts_pkey PRIMARY KEY USING INDEX descriptors_scripts_pkey;
CREATE INDEX descriptors_scripts_code_script ON descriptors_scripts (code, script);

CREATE TABLE IF NOT EXISTS wallets (
  wallet_id TEXT NOT NULL PRIMARY KEY,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP);

CREATE TABLE IF NOT EXISTS scripts_wallets (
code TEXT NOT NULL,
script TEXT NOT NULL,
wallet_id TEXT NOT NULL REFERENCES wallets ON DELETE CASCADE,
PRIMARY KEY (code, script, wallet_id),
FOREIGN KEY (code, script) REFERENCES scripts ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS descriptors_wallets (
code TEXT NOT NULL,
descriptor TEXT NOT NULL,
wallet_id TEXT NOT NULL REFERENCES wallets ON DELETE CASCADE,
PRIMARY KEY (code, descriptor, wallet_id),
FOREIGN KEY (code, descriptor) REFERENCES descriptors ON DELETE CASCADE
);

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
SELECT o.code, o.tx_id, o.idx, o.script, o.value, MAX(ob.height) height FROM outs o
INNER JOIN txs_blks otb USING (code, tx_id)
INNER JOIN blks ob USING (code, blk_id)
LEFT JOIN ins i ON o.code = i.code AND o.tx_id = i.spent_tx_id AND o.idx = i.spent_idx
LEFT JOIN txs_blks itb ON o.code = itb.code AND i.input_tx_id = itb.tx_id
LEFT JOIN blks ib ON o.code = ib.code AND itb.blk_id=ib.blk_id
GROUP BY o.code, o.tx_id, o.idx, o.script, o.value HAVING BOOL_OR(ob.confirmed) = 't' AND (BOOL_OR(ib.confirmed) = 'f' OR BOOL_OR(ib.confirmed) is NULL);

CREATE OR REPLACE VIEW unconf_txs AS
SELECT t.code, t.tx_id FROM txs t
LEFT JOIN txs_blks tb USING (code, tx_id)
LEFT JOIN blks b USING (code, blk_id)
GROUP BY t.code, t.tx_id, t.raw, t.indexed_at HAVING BOOL_AND(b.confirmed) = 'f' OR BOOL_OR(b.confirmed) is null;

CREATE OR REPLACE FUNCTION get_wallet_conf_utxos(in_code TEXT, in_wallet_id TEXT)
RETURNS TABLE (code TEXT, tx_id TEXT, idx INTEGER, script TEXT, value BIGINT) AS $$
  SELECT cu.code, cu.tx_id, cu.idx, cu.script, cu.value
  FROM (SELECT ds.script
  FROM descriptors_wallets dw
  INNER JOIN descriptors_scripts ds ON dw.code = ds.code AND dw.descriptor = ds.descriptor
  WHERE dw.code = in_code AND dw.wallet_id=in_wallet_id
  UNION
  SELECT sw.script
  FROM scripts_wallets sw
  WHERE sw.code = in_code AND sw.wallet_id=in_wallet_id) s
  INNER JOIN conf_utxos cu ON in_code=cu.code AND s.script=cu.script
$$  LANGUAGE SQL STABLE;


CREATE OR REPLACE VIEW tracked_scripts AS
SELECT s.code, s.script, s.addr, dw.wallet_id, 'DESCRIPTOR' source, ds.descriptor, ds.keypath, ds.idx
FROM descriptors_scripts ds
INNER JOIN scripts s USING (code, script)
INNER JOIN descriptors_wallets dw USING (code, descriptor)
UNION ALL
SELECT s.code, s.script, s.addr, sw.wallet_id, 'EXPLICIT' source, NULL, NULL, NULL
FROM scripts_wallets sw
INNER JOIN scripts s USING (code, script)
