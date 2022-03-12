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
  PRIMARY KEY (code, tx_id),
 /*  FOREIGN KEY (code, replaced_by) REFERENCES txs ON DELETE SET NULL, */
  FOREIGN KEY (code, blk_id) REFERENCES blks ON DELETE SET NULL);

ALTER TABLE txs DROP CONSTRAINT IF EXISTS txs_code_replaced_by_fkey CASCADE;
ALTER TABLE txs ADD CONSTRAINT txs_code_replaced_by_fkey FOREIGN KEY (code, replaced_by) REFERENCES txs (code, tx_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS txs_unconf_idx ON txs (code) INCLUDE (tx_id) WHERE mempool IS TRUE;

CREATE OR REPLACE FUNCTION txs_denormalize() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  q RECORD;
BEGIN
  -- Propagate any change to table outs, ins, and ins_outs
  FOR q IN SELECT t.code, t.tx_id, t.blk_id, t.blk_idx, t.mempool, t.replaced_by, t.seen_at
	FROM new_txs
	JOIN txs t USING (code, tx_id)
  LOOP
	UPDATE outs o SET  blk_id = q.blk_id, blk_idx = q.blk_idx, mempool = q.mempool, replaced_by = q.replaced_by, seen_at = q.seen_at
	WHERE o.code=q.code AND o.tx_id=q.tx_id;

	UPDATE ins i SET  blk_id = q.blk_id, blk_idx = q.blk_idx, mempool = q.mempool, replaced_by = q.replaced_by, seen_at = q.seen_at
	WHERE i.code=q.code AND i.input_tx_id=q.tx_id;

	UPDATE ins_outs io SET  blk_id = q.blk_id, blk_idx = q.blk_idx, mempool = q.mempool, replaced_by = q.replaced_by, seen_at = q.seen_at
	WHERE io.code=q.code AND io.tx_id=q.tx_id;
  END LOOP;
  RETURN NULL;
END
$$;

CREATE TRIGGER txs_insert_trigger
  AFTER UPDATE ON txs
  REFERENCING NEW TABLE AS new_txs
  FOR EACH STATEMENT EXECUTE PROCEDURE txs_denormalize();


-- Get the tip (Note we don't returns blks directly, since it prevent function inlining)
CREATE OR REPLACE FUNCTION get_tip(in_code TEXT)
RETURNS TABLE(code TEXT, blk_id TEXT, height BIGINT) AS $$
  SELECT code, blk_id, height FROM blks WHERE code=in_code AND confirmed IS TRUE ORDER BY height DESC LIMIT 1
$$  LANGUAGE SQL STABLE;

-- Update outs.immature, txs.mempool and txs.replaced_by when a new block comes
CREATE OR REPLACE PROCEDURE new_block_updated(in_code TEXT, coinbase_maturity BIGINT)
AS $$
DECLARE
  maturity_height BIGINT;
  r RECORD;
BEGIN
	maturity_height := (SELECT height - coinbase_maturity + 1 FROM get_tip(in_code));
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
	SELECT t.code, t.tx_id, b.blk_id, bt.blk_idx FROM txs t
	JOIN blks_txs bt USING (code, tx_id)
	JOIN blks b ON b.code=bt.code AND b.blk_id=bt.blk_id
	-- Theorically, we shouldn't use t.mempool IS TRUE. But this call would slow everything down.
	-- Here we depend on the indexer to do the right thing
	WHERE t.code=in_code AND t.mempool IS TRUE AND b.confirmed IS TRUE)
	UPDATE txs t SET mempool='f', replaced_by=NULL, blk_id=q.blk_id, blk_idx=q.blk_idx
	FROM q
	WHERE t.code=q.code AND t.tx_id=q.tx_id;

	-- Turn mempool flag of txs with inputs spent by confirmed blocks to false
	FOR r IN
	  WITH q AS (
	  SELECT mempool_ins.code, mempool_ins.tx_id mempool_tx_id, confirmed_ins.tx_id confirmed_tx_id
	  FROM 
		(SELECT i.code, i.spent_tx_id, i.spent_idx, t.tx_id FROM ins i
		JOIN txs t ON t.code=i.code AND t.tx_id=i.input_tx_id
		WHERE i.code=in_code AND t.mempool IS TRUE) mempool_ins
	  LEFT JOIN (
		SELECT i.code, i.spent_tx_id, i.spent_idx, t.tx_id FROM ins i
		JOIN txs t ON t.code=i.code AND t.tx_id=i.input_tx_id
		WHERE i.code=in_code AND t.blk_id IS NOT NULL
	  ) confirmed_ins USING (code, spent_tx_id, spent_idx)
	  WHERE confirmed_ins.tx_id IS NOT NULL) -- The use of LEFT JOIN is intentional, it forces postgres to use a specific index
	  UPDATE txs t SET mempool='f', replaced_by=q.confirmed_tx_id
	  FROM q
	  WHERE t.code=q.code AND t.tx_id=q.mempool_tx_id
	  RETURNING t.code, t.tx_id, t.replaced_by
	LOOP
	  -- Update also the replaced_by of all descendants
	  WITH RECURSIVE cte(code, tx_id) AS
	  (
		  SELECT  t.code, t.tx_id FROM txs t
		  WHERE
			  t.code=r.code AND
			  t.tx_id=r.tx_id
		  UNION
		  SELECT t.code, t.tx_id FROM cte c
		  JOIN outs o USING (code, tx_id)
		  JOIN ins i ON i.code=c.code AND i.spent_tx_id=o.tx_id AND i.spent_idx=o.idx
		  JOIN txs t ON t.code=c.code AND t.tx_id=i.input_tx_id
		  WHERE t.code=c.code AND t.mempool IS TRUE
	  )
	  UPDATE txs t SET mempool='f', replaced_by=r.replaced_by
	  FROM cte
	  WHERE cte.code=t.code AND cte.tx_id=t.tx_id;
	END LOOP;



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
	JOIN blks_txs bt USING (code, tx_id)
	JOIN blks b ON b.code=bt.code AND b.blk_id=bt.blk_id
	WHERE t.code=in_code AND b.height >= in_height AND b.confirmed IS TRUE AND t.mempool IS FALSE)
	UPDATE txs t
	SET mempool='t', blk_id=NULL, blk_idx=NULL
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


CREATE TABLE IF NOT EXISTS blks_txs (
  code TEXT NOT NULL,
  blk_id TEXT NOT NULL,
  tx_id TEXT NOT NULL,
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
  next_index BIGINT DEFAULT 0,
  PRIMARY KEY (code, descriptor)
);

CREATE TABLE IF NOT EXISTS descriptors_scripts (
  code TEXT NOT NULL,
  descriptor TEXT NOT NULL,
  idx BIGINT NOT NULL,
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

CREATE OR REPLACE FUNCTION get_wallets_histogram(in_from TIMESTAMPTZ, in_to TIMESTAMPTZ, in_interval INTERVAL)
RETURNS TABLE(date TIMESTAMPTZ, wallet_id TEXT, code TEXT, asset_id TEXT, balance BIGINT) AS $$
  SELECT s AS time, q.wallet_id, q.code, q.asset_id, q.change + COALESCE((SELECT balance_total FROM wallets_history WHERE in_from <= seen_at LIMIT 1), 0) AS balance
  FROM generate_series(in_from,
					   in_to - in_interval,
					   in_interval) s,
  LATERAL (
	  SELECT wallet_id, code, asset_id, COALESCE(SUM(balance_change),0) change FROM wallets_history
	  WHERE  s <= seen_at AND seen_at < s + in_interval
	  GROUP BY wallet_id, code, asset_id
  ) q
$$  LANGUAGE SQL STABLE;

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
