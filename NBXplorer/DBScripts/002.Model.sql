CREATE TABLE IF NOT EXISTS blks (
  code TEXT NOT NULL,
  blk_id TEXT NOT NULL,
  height BIGINT, prev_id TEXT NOT NULL,
  confirmed BOOLEAN DEFAULT 't',
  indexed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, blk_id));
CREATE INDEX IF NOT EXISTS blks_code_height_idx ON blks (code, height DESC) WHERE confirmed IS TRUE;

CREATE TABLE IF NOT EXISTS txs (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  raw BYTEA DEFAULT NULL,
  blk_id TEXT DEFAULT NULL,
  blk_idx INT DEFAULT NULL,
  mempool BOOLEAN DEFAULT 't',
  replaced_by TEXT DEFAULT NULL,
  seen_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
 /*  PRIMARY KEY (code, tx_id) Handled by index below , */
 /*  FOREIGN KEY (code, replaced_by) REFERENCES txs ON DELETE SET NULL, */
  FOREIGN KEY (code, blk_id) REFERENCES blks ON DELETE SET NULL);

CREATE UNIQUE INDEX IF NOT EXISTS txs_pkey ON txs (code, tx_id) INCLUDE (blk_id, mempool, replaced_by);

ALTER TABLE txs DROP CONSTRAINT IF EXISTS txs_pkey CASCADE;
ALTER TABLE txs ADD CONSTRAINT txs_pkey PRIMARY KEY USING INDEX txs_pkey;

ALTER TABLE txs DROP CONSTRAINT IF EXISTS txs_code_replaced_by_fkey CASCADE;
ALTER TABLE txs ADD CONSTRAINT txs_code_replaced_by_fkey FOREIGN KEY (code, replaced_by) REFERENCES txs (code, tx_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS txs_unconf_idx ON txs (code) INCLUDE (tx_id) WHERE mempool IS TRUE;

-- Update outs.immature, txs.mempool and txs.replaced_by when a new block comes
CREATE OR REPLACE PROCEDURE new_block_updated(in_code TEXT, coinbase_maturity INT)
AS $$
DECLARE
  maturity_height BIGINT;
BEGIN
	maturity_height := (SELECT MAX(height) - coinbase_maturity + 1 FROM blks WHERE code='BTC' AND confirmed IS TRUE);
	-- Turn immature flag of outputs to mature
	-- Note that we never set the outputs back to immature, even in reorg
	-- But that's such a corner case that we don't care.
	WITH q AS (
	  SELECT o.code, o.tx_id, o.idx FROM outs o
	  JOIN txs t USING (code, tx_id)
	  JOIN blks b ON b.code=o.code AND b.blk_id=t.blk_id
	  WHERE o.code=in_code AND o.immature IS TRUE AND b.height < maturity_height
	)
	UPDATE outs o SET immature='f' 
	FROM q
	WHERE o.code=q.code AND o.tx_id=q.tx_id AND o.idx=q.idx;

	-- Turn mempool flag of confirmed txs to false
	WITH q AS (
	SELECT t.code, t.tx_id, b.blk_id FROM txs t
	JOIN txs_blks tb USING (code, tx_id)
	JOIN blks b ON b.code=tb.code AND b.blk_id=tb.blk_id
	WHERE t.code=in_code AND mempool IS TRUE AND b.confirmed IS TRUE)
	UPDATE txs t SET mempool='f', replaced_by=NULL, blk_id=q.blk_id
	FROM q
	WHERE t.code=q.code AND t.tx_id=q.tx_id;

	-- Turn mempool flag of txs with inputs spent by confirmed blocks to false
	WITH q AS (
	SELECT mempool_ins.code, mempool_ins.tx_id mempool_tx_id, confirmed_ins.tx_id confirmed_tx_id
	FROM 
	  (SELECT i.code, i.spent_tx_id, t.tx_id FROM ins i
	  JOIN txs t ON t.code=i.code AND t.tx_id=i.input_tx_id
	  WHERE i.code=in_code AND t.mempool IS TRUE) mempool_ins
	LEFT JOIN (
	  SELECT i.code, i.spent_tx_id, t.tx_id FROM ins i
	  JOIN txs t ON t.code=i.code AND t.tx_id=i.input_tx_id
	  WHERE i.code=in_code AND t.blk_id IS NOT NULL
	) confirmed_ins USING (code, spent_tx_id)
	WHERE confirmed_ins.tx_id IS NOT NULL) -- The use of LEFT JOIN is intentional, it forces postgres to use a specific index
	UPDATE txs t SET mempool='f', replaced_by=q.confirmed_tx_id
	FROM q
	WHERE t.code=q.code AND t.tx_id=q.mempool_tx_id;


	-- Turn spent_blk_id for outputs which has been spent
	WITH q AS (
	SELECT o.code, o.tx_id, o.idx, ti.blk_id FROM outs o
	JOIN ins i ON i.code=o.code AND i.spent_tx_id=o.tx_id AND i.spent_idx=o.idx
	JOIN txs ti ON i.code=ti.code AND i.input_tx_id=ti.tx_id
	WHERE o.code=in_code AND o.spent_blk_id IS NULL AND ti.blk_id IS NOT NULL)
	UPDATE outs o
	SET spent_blk_id=q.blk_id
	FROM q
	WHERE q.code=o.code AND q.tx_id=o.tx_id AND q.idx=o.idx;
END;
$$ LANGUAGE plpgsql;

-- Orphan blocks at `in_height` or above.
-- This bring back orphaned transactions in mempool
CREATE OR REPLACE PROCEDURE orphan_blocks(in_code TEXT, in_height BIGINT)
AS $$
DECLARE
BEGIN
	-- Set mempool flags of the txs in the blocks back to true
	WITH q AS (
	SELECT t.code, t.tx_id FROM txs t
	JOIN txs_blks tb USING (code, tx_id)
	JOIN blks b ON b.code=tb.code AND b.blk_id=tb.blk_id
	WHERE t.code=in_code AND b.height >= in_height AND b.confirmed IS TRUE AND t.mempool IS FALSE)
	UPDATE txs t
	SET mempool='t', blk_id=NULL
	FROM q
	WHERE q.code=t.code AND q.tx_id=t.tx_id;

	-- Set spent_blk_id of outs that where spent back to null
	WITH q AS (
	  SELECT o.code, o.tx_id, o.idx FROM outs o
	  JOIN blks b ON o.code=b.code AND spent_blk_id=b.blk_id
	  WHERE o.code=in_code AND b.height >= in_height AND confirmed IS TRUE
	)
	UPDATE outs o
	SET spent_blk_id=NULL
	FROM q
	WHERE q.code=o.code AND q.tx_id=o.tx_id AND q.idx=o.idx;

	-- Set confirmed flags of blks to false
	UPDATE blks
	SET confirmed='f'
	WHERE code=in_code AND height >= in_height;
END;
$$ LANGUAGE plpgsql;


CREATE TABLE IF NOT EXISTS txs_blks (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  blk_id TEXT NOT NULL,
  blk_idx INT DEFAULT NULL,
  PRIMARY KEY(code, tx_id, blk_id),
  FOREIGN KEY(code, tx_id) REFERENCES txs ON DELETE CASCADE,
  FOREIGN KEY(code, blk_id) REFERENCES blks ON DELETE CASCADE);


CREATE TABLE IF NOT EXISTS scripts (
  code TEXT NOT NULL,
  script TEXT NOT NULL,
  addr TEXT NOT NULL,
  used BOOLEAN NOT NULL DEFAULT 'f'
  /* PRIMARY KEY(code, script) See index below */
);
ALTER TABLE scripts DROP CONSTRAINT IF EXISTS scripts_pkey CASCADE;
CREATE UNIQUE INDEX IF NOT EXISTS scripts_pkey ON scripts (code, script) INCLUDE (addr, used);
ALTER TABLE scripts ADD CONSTRAINT scripts_pkey PRIMARY KEY USING INDEX scripts_pkey;

CREATE TABLE IF NOT EXISTS outs (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  idx INT NOT NULL,
  script TEXT NOT NULL,
  value BIGINT NOT NULL,
  immature BOOLEAN DEFAULT 'f',
  spent_blk_id TEXT DEFAULT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  /* PRIMARY KEY (code, tx_id, idx) (enforced with index), */
  FOREIGN KEY (code, spent_blk_id) REFERENCES blks (code, blk_id) ON DELETE SET NULL,
  FOREIGN KEY (code, tx_id) REFERENCES txs ON DELETE CASCADE,
  FOREIGN KEY (code, script) REFERENCES scripts ON DELETE CASCADE);

CREATE INDEX IF NOT EXISTS outs_code_immature_idx ON outs (code) INCLUDE (tx_id, idx) WHERE immature IS TRUE;
CREATE INDEX IF NOT EXISTS outs_unspent_idx ON outs (code) WHERE spent_blk_id IS NULL;

CREATE OR REPLACE FUNCTION set_scripts_used()
  RETURNS TRIGGER 
  LANGUAGE PLPGSQL
AS $$
BEGIN
	UPDATE scripts
	  SET used='t'
	  WHERE code=NEW.code AND script=NEW.script;
	RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS outs_set_scripts_used on outs;
CREATE TRIGGER outs_set_scripts_used AFTER INSERT ON outs FOR EACH ROW EXECUTE PROCEDURE set_scripts_used();
  
ALTER TABLE outs DROP CONSTRAINT IF EXISTS outs_pkey CASCADE;
CREATE UNIQUE INDEX IF NOT EXISTS outs_pkey ON outs (code, tx_id, idx) INCLUDE (script, value);
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
  input_idx TEXT NOT NULL,
  spent_tx_id TEXT NOT NULL,
  spent_idx INT NOT NULL,
  PRIMARY KEY (code, input_tx_id, input_idx),
  FOREIGN KEY (code, spent_tx_id, spent_idx) REFERENCES outs (code, tx_id, idx) ON DELETE CASCADE,
  FOREIGN KEY (code, input_tx_id) REFERENCES txs (code, tx_id) ON DELETE CASCADE,
  FOREIGN KEY (code, spent_tx_id) REFERENCES txs (code, tx_id) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS ins_code_spentoutpoint_txid_idx ON ins (code, spent_tx_id, spent_idx) INCLUDE (input_tx_id, input_idx);

CREATE TABLE IF NOT EXISTS descriptors (
  code TEXT NOT NULL,
  descriptor TEXT NOT NULL,
  next_index INT DEFAULT 0,
  PRIMARY KEY (code, descriptor)
);

CREATE TABLE IF NOT EXISTS descriptors_scripts (
  code TEXT NOT NULL,
  descriptor TEXT NOT NULL,
  idx INT NOT NULL,
  script TEXT NOT NULL,
  keypath TEXT NOT NULL,
  /* PRIMARY KEY (code, descriptor, idx) , Enforced via index */
  FOREIGN KEY (code, script) REFERENCES scripts ON DELETE CASCADE
);
ALTER TABLE descriptors_scripts DROP CONSTRAINT IF EXISTS descriptors_scripts_pkey CASCADE;
CREATE UNIQUE INDEX IF NOT EXISTS descriptors_scripts_pkey ON descriptors_scripts (code, descriptor, idx) INCLUDE (script);
ALTER TABLE descriptors_scripts ADD CONSTRAINT descriptors_scripts_pkey PRIMARY KEY USING INDEX descriptors_scripts_pkey;

CREATE INDEX IF NOT EXISTS descriptors_scripts_code_script ON descriptors_scripts (code, script);

CREATE TABLE IF NOT EXISTS wallets (
  wallet_id TEXT NOT NULL PRIMARY KEY,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP);

CREATE TABLE IF NOT EXISTS wallets_scripts (
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

CREATE TABLE IF NOT EXISTS wallet_metadata (
wallet_id TEXT NOT NULL REFERENCES wallets ON DELETE CASCADE,
key TEXT NOT NULL,
data JSONB NOT NULL,
PRIMARY KEY (wallet_id, key)
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

CREATE OR REPLACE VIEW tracked_scripts AS
SELECT s.code, s.script, s.addr, dw.wallet_id, 'DESCRIPTOR' source, ds.descriptor, ds.keypath, ds.idx
FROM descriptors_scripts ds
INNER JOIN scripts s USING (code, script)
INNER JOIN descriptors_wallets dw USING (code, descriptor)
UNION ALL
SELECT s.code, s.script, s.addr, ws.wallet_id, 'EXPLICIT' source, NULL, NULL, NULL
FROM wallets_scripts ws
INNER JOIN scripts s USING (code, script);


CREATE OR REPLACE VIEW ins_outs AS
SELECT * FROM 
(SELECT o.code, o.tx_id, t.blk_id, 'OUTPUT' source, o.tx_id out_tx_id, o.idx, o.script, o.value, o.immature, t.mempool, t.replaced_by, t.seen_at
FROM outs o
JOIN txs t USING (code, tx_id)
UNION ALL
SELECT i.code, i.input_tx_id tx_id, t.blk_id, 'INPUT', i.spent_tx_id out_tx_id, i.spent_idx, o.script, o.value, 'f', t.mempool, t.replaced_by, t.seen_at
FROM ins i
JOIN outs o ON i.code=o.code AND i.spent_tx_id=o.tx_id AND i.spent_idx=o.idx
JOIN txs t ON i.code=t.code AND i.input_tx_id=t.tx_id) q
WHERE (blk_id IS NOT NULL OR (mempool IS TRUE AND replaced_by IS NULL));

-- Returns current UTXOs
-- Warning: It also returns the UTXO that are confirmed but spent in the mempool, as well as immature utxos.
--          If you want the available UTXOs which can be spent use 'WHERE spent_mempool IS FALSE AND immature IS FALSE'.

CREATE OR REPLACE VIEW utxos AS
WITH current_ins AS
(
	SELECT i.*, ti.mempool FROM ins i
	JOIN txs ti ON i.code = ti.code AND i.input_tx_id = ti.tx_id
	WHERE ti.blk_id IS NOT NULL OR (ti.mempool IS TRUE AND ti.replaced_by IS NULL)
)
SELECT o.*, ob.blk_id, ob.height, i.input_tx_id spending_tx_id, i.input_idx spending_idx, txo.mempool mempool, (i.mempool IS TRUE) spent_mempool FROM outs o
JOIN txs txo USING (code, tx_id)
LEFT JOIN blks ob USING (code, blk_id)
LEFT JOIN current_ins i ON o.code = i.code AND o.tx_id = i.spent_tx_id AND o.idx = i.spent_idx
WHERE o.spent_blk_id IS NULL AND (txo.blk_id IS NOT NULL OR (txo.mempool IS TRUE AND txo.replaced_by IS NULL)) AND
	  (i.input_tx_id IS NULL OR i.mempool IS TRUE);

CREATE OR REPLACE VIEW wallets_utxos AS
SELECT q.wallet_id, u.* FROM utxos u,
LATERAL (SELECT dw.wallet_id, dw.code, ds.script
		 FROM descriptors_wallets dw
		 INNER JOIN descriptors_scripts ds ON dw.code = ds.code AND dw.descriptor = ds.descriptor
         WHERE ds.code = u.code AND u.script = ds.script
		 UNION
		 SELECT ws.wallet_id, ws.code, ws.script
		 FROM wallets_scripts ws
		 WHERE ws.code=u.code AND u.script=ws.script) q;

CREATE OR REPLACE VIEW wallets_balances AS
SELECT
	wallet_id,
	COALESCE(SUM(value) FILTER (WHERE spent_mempool IS FALSE), 0) unconfirmed_balance,
	COALESCE(SUM(value) FILTER (WHERE blk_id IS NOT NULL), 0) confirmed_balance,
	COALESCE(SUM(value) FILTER (WHERE spent_mempool IS FALSE AND immature IS FALSE), 0) available_balance
FROM wallets_utxos
GROUP BY wallet_id;