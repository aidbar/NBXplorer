﻿-- This script generate a whale wallet with 223000 transactions, used to check query performance.

SET random_page_cost = 1.0;
SET enable_seqscan = off;

INSERT INTO blks
SELECT 'BTC', encode(sha256(('b-' || s)::bytea), 'hex') blk_id, s height, encode(sha256(('b-' || (s-1))::bytea), 'hex') prev_id
FROM generate_series(0, 223000) s;

INSERT INTO txs
SELECT 'BTC', encode(sha256(('t-' || s)::bytea), 'hex') tx_id, NULL, encode(sha256(('b-' || (s))::bytea), 'hex') blk_id, 0, 'f'
FROM generate_series(0, 223000) s;


INSERT INTO scripts
SELECT 'BTC', encode(sha256(('s-' || s)::bytea), 'hex') script, 'lol' addr
FROM generate_series(0, 223000) s;


INSERT INTO outs
SELECT 'BTC', encode(sha256(('t-' || s)::bytea), 'hex') tx_id, 0 idx, encode(sha256(('s-' || s)::bytea), 'hex') script, 40 "value"
FROM generate_series(0, 223000) s
WHERE MOD(s, 2) = 0;

INSERT INTO ins
SELECT 'BTC', encode(sha256(('t-' || s)::bytea), 'hex') input_tx_id, 0 input_idx, encode(sha256(('t-' || (s-1))::bytea), 'hex') spent_tx_id, 0 spent_idx
FROM generate_series(0, 223000) s
WHERE MOD(s, 2) = 1;

INSERT INTO wallets VALUES ('WHALE');
INSERT INTO descriptors VALUES ('BTC', 'WHALEDESC', 0);
INSERT INTO descriptors_wallets VALUES ('BTC', 'WHALEDESC', 'WHALE');

INSERT INTO descriptors_scripts
SELECT 'BTC', 'WHALEDESC', s, encode(sha256(('s-' || s)::bytea), 'hex') script, s
FROM generate_series(0, 223000) s;

CALL new_block_updated('BTC', 100);