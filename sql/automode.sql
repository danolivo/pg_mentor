
LOAD 'pg_mentor';

CREATE TABLE test_automode (
  x integer,
  y text DEFAULT 'some long enough text string'
) WITH (autovacuum_enabled = off);

INSERT INTO test_automode (x) (SELECT x FROM generate_series(1,1E6) AS x);
INSERT INTO test_automode (x) (SELECT 1 FROM generate_series(1,100) AS x);
INSERT INTO test_automode (x) (SELECT 1E6 + x%10 FROM generate_series(1,1E5) AS x);
CREATE INDEX ON test_automode (x);
VACUUM ANALYZE test_automode;
SET pg_mentor.manage_auto_mode = 'off';

PREPARE tst (integer) AS SELECT * FROM test_automode WHERE x = $1;

-- Use medium-case initial executions to raise up average custom plan cost
\o /dev/null
EXPLAIN EXECUTE tst(1) \watch i=0 c=5
\o
EXPLAIN (ANALYZE, COSTS ON, BUFFERS ON, TIMING OFF)
EXECUTE tst(1); -- must switch to generic plan

-- Use worst-case scenario many times. By-default, generic plan will never been
-- reverted to the custom one.
\o /dev/null
EXPLAIN EXECUTE tst(1E6+1) \watch i=0 c=10
\o
EXPLAIN (ANALYZE, COSTS ON, BUFFERS ON, TIMING OFF)
EXECUTE tst(1E6+1);


EXPLAIN (COSTS ON, BUFFERS ON, TIMING OFF, GENERIC_PLAN)
EXECUTE tst(1);

/*
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS ON, TIMING OFF)
SELECT * FROM test_automode WHERE x = 1 \watch i=0 c=10

EXPLAIN (ANALYZE, COSTS OFF, BUFFERS ON, TIMING OFF)
SELECT * FROM test_automode WHERE x = 1E6+1 \watch i=0 c=10
*/