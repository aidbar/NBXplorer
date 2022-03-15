-- This schema is quite simple:
-- The main tables are blks, blks_txs, txs, ins, outs, ins_outs, descriptors, desriptors_scripts, scripts, wallets, wallets_descriptors, wallets_scripts.
-- Those represent common concepts in the UTXO model.
-- Note that the model is heavily denormalized. Columns of txs are present in ins, outs, ins_outs.
-- ins_outs represent the same informations as ins and outs, but in a single table indexed with a timestamp.
-- The denormalization is kept up to date thanks to a bunch of triggers:
-- Changes to txs are propagated to ins, outs, and ins_outs.
-- Changes to ins and outs are propagated to ins_outs.
-- As such, an indexer just have to insert ins/outs/txs and blks without caring about the denormalization.
-- The triggers also detect double spend conditions and manage the value of txs.replaced_by accordingly.
-- There is one materialized view called wallets_history, which provide an history of wallets (time ordered list of wallet_id, code, asset_id, balance-change, total-balance)
-- refreshing this view is quite heavy (It can take between 5 and 10 seconds for huge database).
-- This view is specifically useful for reports and creating histograms via get_wallets_histogram.
-- And indexer is responsible for creating wallets, associating descriptor, deriving scripts and adding them in descriptors_scripts.
-- Add relevant ins/outs. (For now this part is difficult, soon will provide better solution)

CREATE TABLE IF NOT EXISTS blks (
  code TEXT NOT NULL,
  blk_id TEXT NOT NULL,
  height BIGINT, prev_id TEXT NOT NULL,
  confirmed BOOLEAN DEFAULT 'f',
  indexed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, blk_id));
CREATE INDEX IF NOT EXISTS blks_code_height_idx ON blks (code, height DESC) WHERE confirmed IS TRUE;


CREATE OR REPLACE FUNCTION blks_confirmed_update_txs() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
	r RECORD;
	maturity_height BIGINT;
BEGIN
  IF NEW.confirmed = OLD.confirmed THEN
	RETURN NEW;
  END IF;

  IF NEW.confirmed IS TRUE THEN
	-- TODO: We assume 100 blocks for immaturity. We should probably make this data configurable on separate table.
	maturity_height := (SELECT height - 100 + 1 FROM get_tip(NEW.code));
	-- Turn immature flag of outputs to mature
	-- Note that we never set the outputs back to immature, even in reorg
	-- But that's such a corner case that we don't care.
	WITH q AS (
	  SELECT o.code, o.tx_id, o.idx FROM outs o
	  JOIN txs t USING (code, tx_id)
	  JOIN blks b ON b.code=o.code AND b.blk_id=t.blk_id
	  WHERE o.code=NEW.code AND o.immature IS TRUE AND b.height < maturity_height
	)
	UPDATE outs o SET immature='f' 
	FROM q
	WHERE o.code=q.code AND o.tx_id=q.tx_id AND o.idx=q.idx;

	-- Turn mempool flag of confirmed txs to false
	WITH q AS (
	SELECT t.code, t.tx_id, bt.blk_id, bt.blk_idx FROM txs t
	JOIN blks_txs bt USING (code, tx_id)
	WHERE t.code=NEW.code AND bt.blk_id=NEW.blk_id)
	UPDATE txs t SET mempool='f', replaced_by=NULL, blk_id=q.blk_id, blk_idx=q.blk_idx
	FROM q
	WHERE t.code=q.code AND t.tx_id=q.tx_id;

	-- Turn mempool flag of txs with inputs spent by confirmed blocks to false
	WITH q AS (
	SELECT mempool_ins.code, mempool_ins.tx_id mempool_tx_id, confirmed_ins.tx_id confirmed_tx_id
	FROM 
	  (SELECT i.code, i.spent_tx_id, i.spent_idx, t.tx_id FROM ins i
	  JOIN txs t ON t.code=i.code AND t.tx_id=i.input_tx_id
	  WHERE i.code=NEW.code AND t.mempool IS TRUE) mempool_ins
	LEFT JOIN (
	  SELECT i.code, i.spent_tx_id, i.spent_idx, t.tx_id FROM ins i
	  JOIN txs t ON t.code=i.code AND t.tx_id=i.input_tx_id
	  WHERE i.code=NEW.code AND t.blk_id = NEW.blk_id
	) confirmed_ins USING (code, spent_tx_id, spent_idx)
	WHERE confirmed_ins.tx_id IS NOT NULL) -- The use of LEFT JOIN is intentional, it forces postgres to use a specific index
	UPDATE txs t SET mempool='f', replaced_by=q.confirmed_tx_id
	FROM q
	WHERE t.code=q.code AND t.tx_id=q.mempool_tx_id;
  ELSE -- IF not confirmed anymore
	-- Set mempool flags of the txs in the blocks back to true
	WITH q AS (
	  SELECT code, tx_id FROM blks_txs
	  WHERE code=NEW.code AND blk_id=NEW.blk_id
	)
	-- We can't query over txs.blk_id directly, because it doesn't have an index
	UPDATE txs t
	SET mempool='t', blk_id=NULL, blk_idx=NULL
	FROM q
	WHERE t.code=q.code AND t.tx_id = q.tx_id;
  END IF;

  RETURN NEW;
END
$$;

CREATE TRIGGER blks_confirmed_trigger
  AFTER UPDATE ON blks
  FOR EACH ROW EXECUTE PROCEDURE blks_confirmed_update_txs();

CREATE TABLE IF NOT EXISTS txs (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  -- The raw data of transactions isn't really useful aside for book keeping. Indexers can ignore this column and save some space.
  raw BYTEA DEFAULT NULL,
  blk_id TEXT DEFAULT NULL,
  blk_idx INT DEFAULT NULL,
  mempool BOOLEAN DEFAULT 't',
  replaced_by TEXT DEFAULT NULL,
  seen_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, tx_id),
 /*  FOREIGN KEY (code, replaced_by) REFERENCES txs ON DELETE SET NULL, */
  FOREIGN KEY (code, blk_id) REFERENCES blks ON DELETE SET NULL);

ALTER TABLE txs DROP CONSTRAINT IF EXISTS txs_code_replaced_by_fkey CASCADE;
ALTER TABLE txs ADD CONSTRAINT txs_code_replaced_by_fkey FOREIGN KEY (code, replaced_by) REFERENCES txs (code, tx_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS txs_unconf_idx ON txs (code) INCLUDE (tx_id) WHERE mempool IS TRUE;

CREATE OR REPLACE FUNCTION txs_denormalize() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
	r RECORD;
BEGIN
  -- Propagate any change to table outs, ins, and ins_outs
	UPDATE outs o SET  blk_id = NEW.blk_id, blk_idx = NEW.blk_idx, mempool = NEW.mempool, replaced_by = NEW.replaced_by, seen_at = NEW.seen_at
	WHERE o.code=NEW.code AND o.tx_id=NEW.tx_id;

	UPDATE ins i SET  blk_id = NEW.blk_id, blk_idx = NEW.blk_idx, mempool = NEW.mempool, replaced_by = NEW.replaced_by, seen_at = NEW.seen_at
	WHERE i.code=NEW.code AND i.input_tx_id=NEW.tx_id;

	UPDATE ins_outs io SET  blk_id = NEW.blk_id, blk_idx = NEW.blk_idx, mempool = NEW.mempool, replaced_by = NEW.replaced_by, seen_at = NEW.seen_at
	WHERE io.code=NEW.code AND io.tx_id=NEW.tx_id;

	-- Propagate any replaced_by / mempool to ins/outs/ins_outs and to the children
	IF NEW.replaced_by IS DISTINCT FROM OLD.replaced_by THEN
	  FOR r IN 
	  	SELECT code, input_tx_id, replaced_by FROM ins
		WHERE code=NEW.code AND spent_tx_id=NEW.tx_id AND replaced_by IS DISTINCT FROM NEW.replaced_by
	  LOOP
		UPDATE txs SET replaced_by=NEW.replaced_by
		WHERE code=r.code AND tx_id=r.input_tx_id;
	  END LOOP;
	END IF;

	IF NEW.mempool != OLD.mempool THEN
	  FOR r IN 
	  	SELECT code, input_tx_id, mempool FROM ins
		WHERE code=NEW.code AND spent_tx_id=NEW.tx_id AND mempool != NEW.mempool
	  LOOP
		UPDATE txs SET mempool=NEW.mempool
		WHERE code=r.code AND tx_id=r.input_tx_id;
	  END LOOP;
	END IF;

	RETURN NEW;
END
$$;

CREATE TRIGGER txs_insert_trigger
  AFTER UPDATE ON txs
  FOR EACH ROW EXECUTE PROCEDURE txs_denormalize();


-- Get the tip (Note we don't returns blks directly, since it prevent function inlining)
CREATE OR REPLACE FUNCTION get_tip(in_code TEXT)
RETURNS TABLE(code TEXT, blk_id TEXT, height BIGINT, prev_id TEXT) AS $$
  SELECT code, blk_id, height, prev_id FROM blks WHERE code=in_code AND confirmed IS TRUE ORDER BY height DESC LIMIT 1
$$  LANGUAGE SQL STABLE;

CREATE TABLE IF NOT EXISTS blks_txs (
  code TEXT NOT NULL,
  blk_id TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  blk_idx INT DEFAULT NULL,
  PRIMARY KEY(code, tx_id, blk_id),
  FOREIGN KEY(code, tx_id) REFERENCES txs ON DELETE CASCADE,
  FOREIGN KEY(code, blk_id) REFERENCES blks ON DELETE CASCADE);

CREATE INDEX IF NOT EXISTS txs_by_blk_id ON blks_txs (code, blk_id);

CREATE OR REPLACE FUNCTION blks_txs_denormalize() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
	r RECORD;
BEGIN
	IF 
	  (SELECT confirmed FROM blks WHERE code=NEW.code AND blk_id=NEW.blk_id)
	THEN
	  -- Propagate values to txs
	  UPDATE txs
	  SET blk_id=NEW.blk_id, blk_idx=NEW.blk_idx, mempool='f', replaced_by=NULL
	  WHERE code=NEW.code AND tx_id=NEW.tx_id;
	END IF;
	RETURN NEW;
END
$$;

CREATE TRIGGER blks_txs_insert_trigger
  AFTER INSERT ON blks_txs
  FOR EACH ROW EXECUTE PROCEDURE blks_txs_denormalize();

CREATE TABLE IF NOT EXISTS scripts (
  code TEXT NOT NULL,
  script TEXT NOT NULL,
  addr TEXT NOT NULL,
  used BOOLEAN NOT NULL DEFAULT 'f',
  PRIMARY KEY(code, script)
);

CREATE OR REPLACE FUNCTION scripts_set_descriptors_scripts_used() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.used != OLD.used THEN
    UPDATE descriptors_scripts ds SET used='t' WHERE code=NEW.code AND script=NEW.script AND used='f';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER scripts_update_trigger
  AFTER UPDATE ON scripts
  FOR EACH ROW EXECUTE PROCEDURE scripts_set_descriptors_scripts_used();

CREATE TABLE IF NOT EXISTS outs (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  idx BIGINT NOT NULL,
  script TEXT NOT NULL,
  value BIGINT NOT NULL,
  -- Allow multi asset support (Liquid)
  asset_id TEXT NOT NULL DEFAULT '',
  immature BOOLEAN NOT NULL DEFAULT 'f',
  spent_blk_id TEXT DEFAULT NULL,
  -- Denormalized data which rarely change: Must be same as tx
  blk_id TEXT DEFAULT NULL,
  blk_idx INT DEFAULT NULL,
  mempool BOOLEAN DEFAULT 't',
  replaced_by TEXT DEFAULT NULL,
  seen_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  /* PRIMARY KEY (code, tx_id, idx) (enforced with index), */
  FOREIGN KEY (code, spent_blk_id) REFERENCES blks (code, blk_id) ON DELETE SET NULL,
  FOREIGN KEY (code, tx_id) REFERENCES txs ON DELETE CASCADE,
  FOREIGN KEY (code, script) REFERENCES scripts ON DELETE CASCADE);

CREATE INDEX IF NOT EXISTS outs_code_immature_idx ON outs (code) INCLUDE (tx_id, idx) WHERE immature IS TRUE;
CREATE INDEX IF NOT EXISTS outs_unspent_idx ON outs (code) WHERE spent_blk_id IS NULL;

CREATE OR REPLACE FUNCTION outs_denormalize_to_ins_outs() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
BEGIN
  INSERT INTO ins_outs
  SELECT
	o.code,
	o.tx_id,
	o.idx,
	't',
	NULL,
	NULL,
	o.script,
	o.value,
	o.asset_id,
	o.immature,
	t.blk_id,
	t.blk_idx,
	t.mempool,
	t.replaced_by,
	t.seen_at
	FROM new_outs o
	JOIN txs t ON t.code=o.code AND t.tx_id=o.tx_id;

	-- Mark scripts as used
	FOR r IN SELECT * FROM new_outs
	LOOP
	  UPDATE scripts
		SET used='t'
		WHERE code=r.code AND script=r.script;
	END LOOP;
	RETURN NULL;
END
$$;

CREATE OR REPLACE FUNCTION outs_denormalize_from_tx() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
BEGIN
  SELECT * INTO r FROM txs WHERE code=NEW.code AND tx_id=NEW.tx_id;
  NEW.blk_id = r.blk_id;
  NEW.blk_idx = r.blk_idx;
  NEW.mempool = r.mempool;
  NEW.replaced_by = r.replaced_by;
  NEW.seen_at = r.seen_at;
  RETURN NEW;
END
$$;

CREATE TRIGGER outs_before_insert_trigger
  BEFORE INSERT ON outs
  FOR EACH ROW EXECUTE PROCEDURE outs_denormalize_from_tx();

CREATE TRIGGER outs_insert_trigger
  AFTER INSERT ON outs
  REFERENCING NEW TABLE AS new_outs
  FOR EACH STATEMENT EXECUTE PROCEDURE outs_denormalize_to_ins_outs();

CREATE OR REPLACE FUNCTION outs_delete_ins_outs() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM ins_outs io WHERE io.code=OLD.code AND io.tx_id=OLD.tx_id AND io.idx=OLD.idx AND io.is_out IS TRUE;
  DELETE FROM ins_outs io WHERE io.code=OLD.code AND io.spent_tx_id=OLD.tx_id AND io.spent_idx=OLD.idx AND io.is_out IS FALSE;
  RETURN OLD;
END
$$;

CREATE TRIGGER outs_delete_trigger
  BEFORE DELETE ON outs
  FOR EACH ROW EXECUTE PROCEDURE outs_delete_ins_outs();
  
ALTER TABLE outs DROP CONSTRAINT IF EXISTS outs_pkey CASCADE;
CREATE UNIQUE INDEX IF NOT EXISTS outs_pkey ON outs (code, tx_id, idx) INCLUDE (script, value, asset_id);
ALTER TABLE outs ADD CONSTRAINT outs_pkey PRIMARY KEY USING INDEX outs_pkey;

CREATE TABLE IF NOT EXISTS ins (
  code TEXT NOT NULL,
  input_tx_id TEXT NOT NULL,
  input_idx BIGINT NOT NULL,
  spent_tx_id TEXT NOT NULL,
  spent_idx BIGINT NOT NULL,
  -- Denormalized data from the spent outs
  script TEXT NOT NULL,
  value BIGINT NOT NULL,
  asset_id TEXT NOT NULL,
  -- Denormalized data which rarely change: Must be same as tx
  blk_id TEXT DEFAULT NULL,
  blk_idx INT DEFAULT NULL,
  mempool BOOLEAN DEFAULT 't',
  replaced_by TEXT DEFAULT NULL,
  seen_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, input_tx_id, input_idx),
  FOREIGN KEY (code, spent_tx_id, spent_idx) REFERENCES outs (code, tx_id, idx) ON DELETE CASCADE,
  FOREIGN KEY (code, input_tx_id) REFERENCES txs (code, tx_id) ON DELETE CASCADE,
  FOREIGN KEY (code, spent_tx_id) REFERENCES txs (code, tx_id) ON DELETE CASCADE,
  FOREIGN KEY (code, script) REFERENCES scripts ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS ins_code_spentoutpoint_txid_idx ON ins (code, spent_tx_id, spent_idx) INCLUDE (input_tx_id, input_idx);


CREATE OR REPLACE FUNCTION ins_denormalize_to_ins_outs() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
BEGIN
  -- Duplicate the ins into the ins_outs table
  INSERT INTO ins_outs
  SELECT
	i.code,
	i.input_tx_id,
	i.input_idx,
	'f',
	i.spent_tx_id,
	i.spent_idx,
	i.script,
	i.value,
	i.asset_id,
	NULL,
	t.blk_id,
	t.blk_idx,
	t.mempool,
	t.replaced_by,
	t.seen_at
	FROM new_ins i
	JOIN txs t ON t.code=i.code AND t.tx_id=i.input_tx_id;
  RETURN NULL;
END
$$;

CREATE OR REPLACE FUNCTION ins_denormalize_from_txs() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
BEGIN

  -- This look detect double spend by inserting into the spent_outs
  -- table and detecting conflicts
  FOR r IN 
	  INSERT INTO spent_outs AS so VALUES (NEW.code, NEW.spent_tx_id, NEW.spent_idx, NEW.input_tx_id)
	  ON CONFLICT (code, tx_id, idx) DO UPDATE SET spent_by=EXCLUDED.spent_by, prev_spent_by=so.spent_by
	  RETURNING *
  LOOP
	  IF r.prev_spent_by IS NOT NULL AND r.prev_spent_by != r.spent_by THEN
		  -- Make the other tx replaced
		  UPDATE txs t SET replaced_by=r.spent_by
		  WHERE t.code=r.code AND t.tx_id=r.prev_spent_by AND t.mempool IS TRUE;

		  -- Make sure this tx isn't replaced
		  UPDATE txs t SET replaced_by=NULL
		  WHERE t.code=r.code AND t.tx_id=r.spent_by AND t.mempool IS TRUE;

		  -- Propagate to the children
		  WITH RECURSIVE cte(code, tx_id) AS
		  (
			SELECT  t.code, t.tx_id FROM txs t
			WHERE
				t.code=r.code AND
				t.tx_id=r.prev_spent_by
			UNION
			SELECT t.code, t.tx_id FROM cte c
			JOIN outs o USING (code, tx_id)
			JOIN ins i ON i.code=c.code AND i.spent_tx_id=o.tx_id AND i.spent_idx=o.idx
			JOIN txs t ON t.code=c.code AND t.tx_id=i.input_tx_id
			WHERE t.code=c.code AND t.mempool IS TRUE
		  )
		  UPDATE txs t SET replaced_by=r.spent_by
		  FROM cte
		  WHERE cte.code=t.code AND cte.tx_id=t.tx_id;
	  END IF;
  END LOOP;

   -- Take the denormalized values from the associated tx, and spent outs, put them in the inserted
  SELECT * INTO r FROM txs WHERE code=NEW.code AND tx_id=NEW.input_tx_id;
  NEW.blk_id = r.blk_id;
  NEW.blk_id = r.blk_id;
  NEW.mempool = r.mempool;
  NEW.replaced_by = r.replaced_by;
  NEW.seen_at = r.seen_at;
  SELECT * INTO r FROM outs WHERE code=NEW.code AND tx_id=NEW.spent_tx_id AND idx=NEW.spent_idx;
  IF NOT FOUND THEN
	RETURN NULL;
  END IF;
  NEW.script = r.script;
  NEW.value = r.value;
  NEW.asset_id = r.asset_id;
  RETURN NEW;
END
$$;

CREATE TRIGGER ins_before_insert_trigger
  BEFORE INSERT ON ins
  FOR EACH ROW EXECUTE PROCEDURE ins_denormalize_from_txs();

CREATE TRIGGER ins_insert_trigger
  AFTER INSERT ON ins
  REFERENCING NEW TABLE AS new_ins
  FOR EACH STATEMENT EXECUTE PROCEDURE ins_denormalize_to_ins_outs();

CREATE OR REPLACE FUNCTION ins_delete_ins_outs() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM ins_outs io WHERE io.code=OLD.code AND io.tx_id=OLD.input_tx_id AND io.idx=OLD.input_idx AND io.is_out IS FALSE;
  RETURN OLD;
END
$$;

CREATE TRIGGER ins_delete_trigger
  BEFORE DELETE ON ins
  FOR EACH ROW EXECUTE PROCEDURE ins_delete_ins_outs();


CREATE TABLE IF NOT EXISTS descriptors (
  code TEXT NOT NULL,
  descriptor TEXT NOT NULL,
  -- Custom data for the indexer (eg. keypathtemplate)
  metadata JSONB NULL DEFAULT NULL,
  -- next_idx and gap are updated during insertion or update to descriptors_scripts
  next_idx BIGINT DEFAULT 0,
  gap BIGINT DEFAULT 0,
  PRIMARY KEY (code, descriptor)
);

CREATE TABLE IF NOT EXISTS descriptors_scripts (
  code TEXT NOT NULL,
  descriptor TEXT NOT NULL,
  idx BIGINT NOT NULL,
  script TEXT NOT NULL,
  -- Custom data for the indexer (eg. redeem scripts)
  metadata JSONB DEFAULT NULL,
  used BOOLEAN NOT NULL DEFAULT 'f',
  /* PRIMARY KEY (code, descriptor, idx) , Enforced via index */
  FOREIGN KEY (code, script) REFERENCES scripts ON DELETE CASCADE
);
ALTER TABLE descriptors_scripts DROP CONSTRAINT IF EXISTS descriptors_scripts_pkey CASCADE;
CREATE UNIQUE INDEX IF NOT EXISTS descriptors_scripts_pkey ON descriptors_scripts (code, descriptor, idx) INCLUDE (script);
ALTER TABLE descriptors_scripts ADD CONSTRAINT descriptors_scripts_pkey PRIMARY KEY USING INDEX descriptors_scripts_pkey;
CREATE INDEX IF NOT EXISTS descriptors_scripts_code_script ON descriptors_scripts (code, script);

CREATE OR REPLACE FUNCTION descriptors_scripts_after_insert_trigger_proc() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
	SELECT code, descriptor, MAX(idx) idx FROM new_descriptors_scripts
	GROUP BY code, descriptor
  LOOP
	UPDATE descriptors s SET next_idx = r.idx + 1, gap = s.gap + (r.idx + 1 - s.next_idx)
	WHERE code=r.code AND descriptor=r.descriptor AND next_idx < r.idx + 1;
  END LOOP;
  RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION descriptors_scripts_before_insert_trigger_proc() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.used = (SELECT used FROM scripts WHERE code=NEW.code AND script=NEW.script);
  RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION descriptors_scripts_before_insert_or_update_trigger_proc() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  last_idx BIGINT;
BEGIN
  IF NEW.used THEN
	--  [1] [2] [3] [4] [5] then next_idx=6, imagine that 3 is now used, we want to update gap to be 2 (because we still have 2 addresses ahead)
	UPDATE descriptors d
	-- If a new address has been used, then the gap can't go down by definition
	SET gap = next_idx - NEW.idx - 1 -- 6 - 3 - 1 = 2
	WHERE code=NEW.code AND descriptor=NEW.descriptor AND NEW.idx >= next_idx - gap; -- If the gap was 2, then only idx 4 or 5 could have changed anything (index >= 6 - 2)
  ELSE -- If not used anymore
	last_idx := (SELECT MAX(ds.idx) FROM descriptors_scripts ds WHERE ds.code=NEW.code AND ds.descriptor=NEW.descriptor AND ds.used IS TRUE AND ds.idx != NEW.idx);
	UPDATE descriptors d
	-- Say 1 and 3 was used. Then the newest latest used address will be 1 (last_idx) and gap should be 4 (gap = 6 - 1 - 1)
	SET gap = COALESCE(next_idx - last_idx - 1, next_idx)
	-- If the index was less than 1, then it couldn't have changed the gap... except if there is no last_idx
	WHERE code=NEW.code AND descriptor=NEW.descriptor  AND (last_idx IS NULL OR NEW.idx > last_idx); 
  END IF;
  RETURN NEW;
END $$;


CREATE TRIGGER descriptors_scripts_before_insert_trigger
  BEFORE INSERT ON descriptors_scripts
  FOR EACH ROW EXECUTE PROCEDURE descriptors_scripts_before_insert_trigger_proc();

CREATE TRIGGER descriptors_scripts_after_insert_trigger
  AFTER INSERT ON descriptors_scripts
  REFERENCING NEW TABLE AS new_descriptors_scripts
  FOR EACH STATEMENT EXECUTE PROCEDURE descriptors_scripts_after_insert_trigger_proc();

CREATE TRIGGER descriptors_scripts_before_insert_or_update_trigger
  AFTER INSERT OR UPDATE ON descriptors_scripts
  FOR EACH ROW EXECUTE PROCEDURE descriptors_scripts_before_insert_or_update_trigger_proc();

CREATE TABLE IF NOT EXISTS wallets (
  wallet_id TEXT NOT NULL PRIMARY KEY,
  metadata JSONB DEFAULT NULL,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP);

CREATE TABLE IF NOT EXISTS wallets_scripts (
  code TEXT NOT NULL,
  script TEXT NOT NULL,
  wallet_id TEXT REFERENCES wallets ON DELETE CASCADE,
  descriptor TEXT DEFAULT NULL,
  idx BIGINT DEFAULT NULL,
  PRIMARY KEY (code, script, wallet_id),
  FOREIGN KEY (code, script) REFERENCES scripts ON DELETE CASCADE,
  FOREIGN KEY (code, descriptor, idx) REFERENCES descriptors_scripts ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS scripts_by_wallet_id_idx ON wallets_scripts(code, wallet_id);

-- Returns a log of inputs and outputs
-- This table is denormalized to improve performance on queries involving seen_at
-- If you want a view of the current in_outs wihtout conflict use
-- SELECT * FROM ins_outs
-- WHERE blk_id IS NOT NULL OR (mempool IS TRUE AND replaced_by IS NULL)
-- ORDER BY seen_at
CREATE TABLE IF NOT EXISTS ins_outs (
  code TEXT NOT NULL,
  -- The tx_id of the input or output
  tx_id TEXT NOT NULL,
  -- The idx of the input or the output
  idx BIGINT NOT NULL,
  is_out BOOLEAN NOT NULL,
  -- Only available for inputs (is_out IS FALSE)
  spent_tx_id TEXT,
  spent_idx BIGINT,
  ----
  script TEXT NOT NULL,
  value BIGINT NOT NULL,
  asset_id TEXT NOT NULL,
  immature BOOLEAN,
  -- Denormalized data which rarely change: Must be same as tx
  blk_id TEXT DEFAULT NULL,
  blk_idx INT DEFAULT NULL,
  mempool BOOLEAN DEFAULT 't',
  replaced_by TEXT DEFAULT NULL,
  seen_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, tx_id, idx, is_out),
  FOREIGN KEY (code, spent_tx_id, spent_idx) REFERENCES outs (code, tx_id, idx) -- outs_delete_ins_outs trigger will take care of deleting, no CASCADE
);
CREATE INDEX IF NOT EXISTS ins_outs_seen_at_idx ON ins_outs (seen_at);

-- Returns current UTXOs
-- Warning: It also returns the UTXO that are confirmed but spent in the mempool, as well as immature utxos.
--          If you want the available UTXOs which can be spent use 'WHERE spent_mempool IS FALSE AND immature IS FALSE'.
CREATE OR REPLACE VIEW utxos AS
WITH current_ins AS
(
	SELECT i.* FROM ins i
	WHERE i.blk_id IS NOT NULL OR (i.mempool IS TRUE AND i.replaced_by IS NULL)
)
SELECT o.*, ob.height, i.input_tx_id spending_tx_id, i.input_idx spending_idx, (i.mempool IS TRUE) spent_mempool FROM outs o
LEFT JOIN blks ob USING (code, blk_id)
LEFT JOIN current_ins i ON o.code = i.code AND o.tx_id = i.spent_tx_id AND o.idx = i.spent_idx
WHERE o.spent_blk_id IS NULL AND (o.blk_id IS NOT NULL OR (o.mempool IS TRUE AND o.replaced_by IS NULL)) AND
	  (i.input_tx_id IS NULL OR i.mempool IS TRUE);

-- Returns UTXOs with their associate wallet
-- Warning: It also returns the UTXO that are confirmed but spent in the mempool, as well as immature utxos.
--          If you want the available UTXOs which can be spent use 'WHERE spent_mempool IS FALSE AND immature IS FALSE'.
CREATE OR REPLACE VIEW wallets_utxos AS
SELECT q.wallet_id, u.* FROM utxos u,
LATERAL (SELECT ws.wallet_id, ws.code, ws.script
		 FROM wallets_scripts ws
         WHERE ws.code = u.code AND ws.script = u.script) q;

-- Returns the balances of a wallet.
-- Warning: A wallet without any balance may not appear as a row in this view
CREATE OR REPLACE VIEW wallets_balances AS
SELECT
	wallet_id,
	code,
	asset_id,
	-- The balance if all unconfirmed transactions, non-conflicting, were finally confirmed
	COALESCE(SUM(value) FILTER (WHERE spent_mempool IS FALSE), 0) unconfirmed_balance,
	-- The balance only taking into accounts confirmed transactions
	COALESCE(SUM(value) FILTER (WHERE blk_id IS NOT NULL), 0) confirmed_balance,
	-- Same as unconfirmed_balance, removing immature utxos (utxos from a miner aged less than 100 blocks)
	COALESCE(SUM(value) FILTER (WHERE spent_mempool IS FALSE AND immature IS FALSE), 0) available_balance,
	-- The total value of immature utxos (utxos from a miner aged less than 100 blocks)
	COALESCE(SUM(value) FILTER (WHERE immature IS TRUE), 0) immature_balance
FROM wallets_utxos
GROUP BY wallet_id, code, asset_id;

-- Only used for double spending detection
CREATE TABLE IF NOT EXISTS spent_outs (
  code TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  idx TEXT NOT NULL,
  spent_by TEXT NOT NULL,
  prev_spent_by TEXT DEFAULT NULL,
  spent_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code, tx_id, idx),
  FOREIGN KEY (code, spent_by) REFERENCES txs ON DELETE CASCADE,
  FOREIGN KEY (code, prev_spent_by) REFERENCES txs ON DELETE CASCADE
);

-- Provide an history of wallets (time ordered list of wallet_id, code, asset_id, balance-change, total-balance)
-- This view is intensive to compute (for 220K transactions, it takes around 5 seconds)
-- This is meant to be used for reports and histograms.
-- If you want the latest history of a wallet, use get_wallets_recent instead.
CREATE MATERIALIZED VIEW IF NOT EXISTS wallets_history AS
	SELECT q.wallet_id,
		   io.code,
		   io.asset_id,
		   tx_id,
		   io.seen_at,
		   COALESCE(SUM (value) FILTER (WHERE is_out IS TRUE), 0) -  COALESCE(SUM (value) FILTER (WHERE is_out IS FALSE), 0) balance_change,
		   SUM(COALESCE(SUM (value) FILTER (WHERE is_out IS TRUE), 0) -  COALESCE(SUM (value) FILTER (WHERE is_out IS FALSE), 0)) OVER (PARTITION BY wallet_id, io.code, asset_id ORDER BY io.seen_at) balance_total
	FROM ins_outs io,
	LATERAL (SELECT ts.wallet_id, ts.code, ts.script
			 FROM wallets_scripts ts
			 WHERE ts.code = io.code AND ts.script = io.script) q
	WHERE blk_id IS NOT NULL
	GROUP BY wallet_id, io.code, io.asset_id, tx_id, seen_at
	ORDER BY seen_at DESC, tx_id, asset_id
WITH DATA;
CREATE UNIQUE INDEX wallets_history_pk ON wallets_history (wallet_id, code, asset_id, tx_id);
CREATE INDEX wallets_history_by_seen_at ON wallets_history (seen_at);

-- Histogram depends on wallets_history, as such, you should make sure the materialized view is refreshed time for up-to-date histogram.
CREATE OR REPLACE FUNCTION get_wallets_histogram(in_wallet_id TEXT, in_code TEXT, in_asset_id TEXT, in_from TIMESTAMPTZ, in_to TIMESTAMPTZ, in_interval INTERVAL)
RETURNS TABLE(date TIMESTAMPTZ, balance_change BIGINT, balance BIGINT) AS $$
  SELECT s AS time,
  		change,
  		SUM (q.change) OVER (ORDER BY s) + COALESCE((SELECT balance_total FROM wallets_history WHERE seen_at < in_from AND code=in_code AND asset_id=in_asset_id LIMIT 1), 0)  AS balance
  FROM generate_series(in_from,
					   in_to - in_interval,
					   in_interval) s
  LEFT JOIN LATERAL (
	  SELECT s, COALESCE(SUM(balance_change),0) change FROM wallets_history
	  WHERE  s <= seen_at AND seen_at < s + in_interval AND wallet_id=in_wallet_id AND code=in_code AND asset_id=in_asset_id
  ) q USING (s)
$$  LANGUAGE SQL STABLE;

-- Useful view to see what has going on recently in a wallet. Doesn't depends on wallets_history.
CREATE OR REPLACE FUNCTION get_wallets_recent(in_wallet_id TEXT, in_limit INT, in_offset INT)
RETURNS TABLE(wallet_id TEXT, code TEXT, asset_id TEXT, tx_id TEXT, seen_at TIMESTAMPTZ, balance_change BIGINT, balance_total BIGINT) AS $$
	SELECT q.wallet_id, q.code, q.asset_id, q.tx_id, q.seen_at, q.balance_change, COALESCE((q.latest_balance - LAG(balance_change_sum, 1) OVER (ORDER BY seen_at DESC)), q.latest_balance) balance_total FROM
		(SELECT q.*, 
				COALESCE((SELECT unconfirmed_balance FROM wallets_balances WHERE wallet_id=q.wallet_id AND code=q.code AND asset_id=q.asset_id), 0) latest_balance,
				SUM(q.balance_change) OVER (ORDER BY seen_at DESC) balance_change_sum FROM 
			(SELECT q.wallet_id,
				   io.code,
				   io.asset_id,
				   tx_id,
				   io.seen_at,
				   COALESCE(SUM (value) FILTER (WHERE is_out IS TRUE), 0) -  COALESCE(SUM (value) FILTER (WHERE is_out IS FALSE), 0) balance_change
			FROM ins_outs io,
			LATERAL (SELECT ts.wallet_id, ts.code, ts.script
					 FROM wallets_scripts ts
					 WHERE ts.code = io.code AND ts.script = io.script) q
			WHERE (blk_id IS NOT NULL OR (mempool IS TRUE AND replaced_by IS NULL))
			GROUP BY wallet_id, io.code, io.asset_id, tx_id, seen_at HAVING (wallet_id=in_wallet_id)
			ORDER BY seen_at DESC, tx_id, asset_id
			LIMIT in_limit) q
		) q
	OFFSET in_offset
$$ LANGUAGE SQL STABLE;

CREATE TYPE new_out AS (
  tx_id TEXT,
  idx BIGINT,
  script TEXT,
  "value" BIGINT,
  asset_id TEXT
);
CREATE TYPE new_in AS (
  tx_id TEXT,
  idx BIGINT,
  spent_tx_id TEXT,
  spent_idx BIGINT
);

CREATE OR REPLACE PROCEDURE new_txs(in_code TEXT, in_outs new_out[], in_ins new_in[]) LANGUAGE plpgsql AS $$
BEGIN
  CALL new_txs(in_code, in_outs, in_ins, CURRENT_TIMESTAMP);
END $$;

CREATE OR REPLACE PROCEDURE new_txs(in_code TEXT, in_outs new_out[], in_ins new_in[], in_seen_at TIMESTAMPTZ) LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
	SELECT o.* FROM scripts s
	JOIN unnest(in_outs) o USING (script)
	WHERE s.code=in_code
  LOOP
	INSERT INTO txs (code, tx_id) VALUES (in_code, r.tx_id) ON CONFLICT (code, tx_id) DO UPDATE SET seen_at=LEAST(in_seen_at, txs.seen_at);
	INSERT INTO outs (code, tx_id, idx, script, value, asset_id) VALUES (in_code, r.tx_id, r.idx, r.script, r.value, r.asset_id) ON CONFLICT DO NOTHING;
  END LOOP;

  -- We need to preserve order of insertion of the inputs since it is important for double spend detection
  FOR r IN
	SELECT i.* FROM  unnest(in_ins) WITH ORDINALITY AS i(tx_id, idx, spent_tx_id, spent_idx, "order"),
	LATERAL (SELECT * FROM outs o WHERE o.code=in_code AND o.tx_id=i.spent_tx_id AND o.idx=i.spent_idx) o
	ORDER BY "order"
  LOOP
	INSERT INTO txs (code, tx_id) VALUES (in_code, r.tx_id) ON CONFLICT (code, tx_id) DO UPDATE SET seen_at=LEAST(in_seen_at, txs.seen_at);
	INSERT INTO ins (code, input_tx_id, input_idx, spent_tx_id, spent_idx) VALUES (in_code, r.tx_id, r.idx, r.spent_tx_id, r.spent_idx) ON CONFLICT DO NOTHING;
  END LOOP;
END $$;