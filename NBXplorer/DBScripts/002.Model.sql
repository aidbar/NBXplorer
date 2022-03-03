CREATE TABLE IF NOT EXISTS blks (
  code TEXT NOT NULL,
  blk_id TEXT NOT NULL,
  height BIGINT, prev_id TEXT NOT NULL,
  confirmed BOOLEAN DEFAULT 't',
  indexed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, blk_id));
CREATE INDEX IF NOT EXISTS blks_code_height_idx ON blks (code, height DESC) WHERE confirmed = 't';
CREATE INDEX IF NOT EXISTS blks_code_blk_id_confirmed_height_idx ON blks (code, blk_id, confirmed, height);

CREATE TABLE IF NOT EXISTS txs (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  raw BYTEA DEFAULT NULL,
  blk_id TEXT DEFAULT NULL,
  seen_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
 /*  PRIMARY KEY (code, tx_id) Handled by index below , */
  FOREIGN KEY (code, blk_id) REFERENCES blks ON DELETE SET NULL);

ALTER TABLE txs DROP CONSTRAINT IF EXISTS txs_pkey CASCADE;
CREATE UNIQUE INDEX IF NOT EXISTS txs_pkey ON txs (code, tx_id) INCLUDE (blk_id);
ALTER TABLE txs ADD CONSTRAINT txs_pkey PRIMARY KEY USING INDEX txs_pkey;

CREATE INDEX IF NOT EXISTS txs_unconf ON txs (code) INCLUDE (tx_id) WHERE blk_id IS NULL;

-- If the blks confirmation state change, set tx.blk_id accordingly
CREATE OR REPLACE FUNCTION set_tx_blk_id()
  RETURNS TRIGGER 
  LANGUAGE PLPGSQL
AS $$
BEGIN
	IF NEW.confirmed THEN
	  UPDATE txs t 
	  SET blk_id=NEW.blk_id
	  FROM (SELECT tb.tx_id FROM txs_blks tb WHERE code=NEW.code AND blk_id=NEW.blk_id) AS q  
	  WHERE t.code=NEW.code AND t.tx_id=q.tx_id;
	ELSE
	  UPDATE txs t 
	  SET blk_id=NULL
	  FROM (SELECT tb.tx_id FROM txs_blks tb WHERE code=NEW.code AND blk_id=NEW.blk_id) AS q  
	  WHERE t.code=NEW.code AND t.tx_id=q.tx_id AND t.blk_id=NEW.blk_id;
	END IF;
	RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS blks_tx_blk_id on blks;
CREATE TRIGGER blks_tx_blk_id AFTER UPDATE ON blks FOR EACH ROW EXECUTE PROCEDURE set_tx_blk_id();
--------------------


-- If the a txs get added to a block, update the mempool flag of the txs
CREATE TABLE IF NOT EXISTS txs_blks (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  blk_id TEXT NOT NULL,
  PRIMARY KEY(code, tx_id, blk_id),
  FOREIGN KEY(code, tx_id) REFERENCES txs ON DELETE CASCADE,
  FOREIGN KEY(code, blk_id) REFERENCES blks ON DELETE CASCADE);

CREATE OR REPLACE FUNCTION set_tx_blk_id2()
  RETURNS TRIGGER 
  LANGUAGE PLPGSQL
AS $$
BEGIN
	IF EXISTS (SELECT b.blk_id FROM blks b WHERE b.blk_id=NEW.blk_id AND b.confirmed='t') THEN
	  UPDATE txs t 
	  SET blk_id=NEW.blk_id
	  FROM (SELECT tb.tx_id FROM txs_blks tb WHERE code=NEW.code AND blk_id=NEW.blk_id) AS q  
	  WHERE t.code=NEW.code AND t.tx_id=q.tx_id;
	ELSE
	  UPDATE txs t 
	  SET blk_id=NULL
	  FROM (SELECT tb.tx_id FROM txs_blks tb WHERE code=NEW.code AND blk_id=NEW.blk_id) AS q  
	  WHERE t.code=NEW.code AND t.tx_id=q.tx_id AND t.blk_id=NEW.blk_id;
	END IF;
	RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS txs_blks_blk_id on txs_blks;
CREATE TRIGGER txs_blks_blk_id AFTER INSERT ON txs_blks FOR EACH ROW EXECUTE PROCEDURE set_tx_blk_id2();


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
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  /* PRIMARY KEY (code, tx_id, idx) (enforced with index), */
  FOREIGN KEY (code, tx_id) REFERENCES txs ON DELETE CASCADE,
  FOREIGN KEY (code, script) REFERENCES scripts ON DELETE CASCADE);

CREATE INDEX IF NOT EXISTS outs_code_immature ON outs (code) INCLUDE (tx_Id, idx) WHERE immature='t';

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

CREATE OR REPLACE VIEW conf_utxos AS
SELECT o.*, ob.blk_id, ob.height FROM outs o
INNER JOIN txs txo USING (code, tx_id)
INNER JOIN blks ob USING (code, blk_id)
LEFT JOIN ins i ON o.code = i.code AND o.tx_id = i.spent_tx_id AND o.idx = i.spent_idx
LEFT JOIN txs ti ON o.code = ti.code AND i.input_tx_id = ti.tx_id
WHERE txo.blk_id IS NOT NULL AND ti.blk_id IS NULL;

CREATE OR REPLACE VIEW unconf_txs AS
SELECT t.code, t.tx_id FROM txs t
WHERE t.blk_id IS NULL;

CREATE OR REPLACE FUNCTION get_wallet_conf_utxos(in_code TEXT, in_wallet_id TEXT)
RETURNS TABLE (code TEXT, tx_id TEXT, idx INTEGER, script TEXT, value BIGINT, immature BOOLEAN, blk_id TEXT, height BIGINT) AS $$
  SELECT cu.code, cu.tx_id, cu.idx, cu.script, cu.value, cu.immature, cu.blk_id, cu.height
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
INNER JOIN scripts s USING (code, script);


CREATE OR REPLACE VIEW ins_outs AS
SELECT o.code, o.tx_id, t.blk_id, 'OUTPUT' source, o.tx_id out_tx_id, o.idx, o.script, o.value, o.immature, t.seen_at
FROM outs o
JOIN txs t USING (code, tx_id)
UNION ALL
SELECT i.code, i.input_tx_id tx_id, t.blk_id, 'INPUT', i.spent_tx_id out_tx_id, i.spent_idx, o.script, o.value, 'f', t.seen_at
FROM ins i
JOIN outs o ON i.code=o.code AND i.spent_tx_id=o.tx_id AND i.spent_idx=o.idx
JOIN txs t ON i.code=t.code AND i.input_tx_id=t.tx_id;

-- We could replace this with a simple UPDATE, but it doesn't take the right index.
-- In practice, this function will rarely modify any data, this make sure the index outs_code_immature is used.
CREATE OR REPLACE FUNCTION set_maturity_below_height(in_code TEXT, in_height INT) RETURNS INT AS
$BODY$
DECLARE
  r RECORD;
  rowUpdated INT;
BEGIN
  rowUpdated = 0;
  FOR r IN SELECT o.tx_id, o.idx FROM outs o
		   JOIN txs t USING (code, tx_id)
		   JOIN blks b ON b.code=o.code AND b.blk_id=t.blk_id
		   WHERE o.code=in_code AND o.immature='t' AND b.height < in_height
  LOOP
	-- This look sub-optimal, but maturity is such a corned case that this line will rarely get executed.
	-- even for a miner, it would be executed a few time per day.
	UPDATE outs SET immature='f' WHERE code=in_code AND tx_id=r.tx_id AND idx=r.idx;
	rowUpdated = rowUpdated + 1;
  END LOOP;
  RETURN rowUpdated;
END
$BODY$  LANGUAGE plpgsql;