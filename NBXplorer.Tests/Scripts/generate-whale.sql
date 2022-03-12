-- This script generate a whale wallet with 223000 transactions, used to check query performance.
INSERT INTO blks
SELECT 'BTC', encode(sha256(('b-' || s)::bytea), 'hex') blk_id, s height, encode(sha256(('b-' || (s-1))::bytea), 'hex') prev_id
FROM generate_series(0, 223000) s;

INSERT INTO txs
SELECT 'BTC', encode(sha256(('t-' || s)::bytea), 'hex') tx_id, NULL, NULL, 0, 't', NULL, CURRENT_TIMESTAMP -  interval '1 minute' * (223000 - s)
FROM generate_series(0, 223000) s;

INSERT INTO blks_txs
SELECT 'BTC', encode(sha256(('b-' || s)::bytea), 'hex') blk_id, encode(sha256(('t-' || s)::bytea), 'hex') tx_id, 0
FROM generate_series(0, 223000) s;

INSERT INTO scripts
SELECT 'BTC', encode(sha256(('s-' || s)::bytea), 'hex') script, 'lol' addr
FROM generate_series(0, 223000) s;


INSERT INTO outs
SELECT 'BTC', encode(sha256(('t-' || s)::bytea), 'hex') tx_id, 0 idx, encode(sha256(('s-' || s)::bytea), 'hex') script, 40 "value"
FROM generate_series(0, 223000) s
WHERE MOD(s, 2) = 0;

INSERT INTO ins
SELECT 'BTC', encode(sha256(('t-' || s)::bytea), 'hex') input_tx_id, 0 input_idx, encode(sha256(('t-' || (s-1))::bytea), 'hex') spent_tx_id, 0 spent_idx, encode(sha256(('s-' || s-1)::bytea), 'hex'), 40, ''
FROM generate_series(0, 223000) s
WHERE MOD(s, 2) = 1;

INSERT INTO wallets VALUES ('WHALE');
INSERT INTO descriptors VALUES ('BTC', 'WHALEDESC', 0);

INSERT INTO descriptors_scripts
SELECT 'BTC', 'WHALEDESC', s, encode(sha256(('s-' || s)::bytea), 'hex') script, s
FROM generate_series(0, 223000) s;

INSERT INTO wallets_scripts
SELECT 'BTC', encode(sha256(('s-' || s)::bytea), 'hex') script, 'WHALE', 'WHALEDESC', s
FROM generate_series(0, 223000) s;

CALL new_block_updated('BTC', 100);
ANALYZE;
REFRESH MATERIALIZED VIEW wallets_history;
ANALYZE;
