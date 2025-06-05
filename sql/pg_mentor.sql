/*
 * Test in this module is based on execution and planning time values. By
 * default it may be unstable. We tune queries as much is possible to be stable.
 * But still, don't care immediately, if results comparison is failed.
 */
CREATE EXTENSION pg_mentor CASCADE;

CREATE FUNCTION show_entries()
RETURNS TABLE (
  refcounter		integer,
  plan_cache_mode	integer,
  calls				bigint,
  query				text
)  AS $$
BEGIN
  RETURN QUERY
    SELECT p.refcounter,p.plan_cache_mode,s.calls,s.query
    FROM pg_mentor_show_prepared_statements(-1) p JOIN
	  pg_stat_statements s USING (queryid);
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION expln(query_string text) RETURNS SETOF text AS $$
BEGIN
    RETURN QUERY
        EXECUTE format('EXPLAIN (VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF) %s', query_string);
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION get_queryId(query_string text) RETURNS bigint AS $$
DECLARE
  res     json;
  queryId bigint;
BEGIN
  EXECUTE format('EXPLAIN (VERBOSE, COSTS OFF, FORMAT JSON) %s', query_string)
  INTO res;

  SELECT res->0->>'Query Identifier' INTO queryId;
  RETURN queryId;
END;
$$ LANGUAGE PLPGSQL;

CREATE TABLE test(x integer);
VACUUM ANALYZE test;

PREPARE stmt1(integer) AS SELECT * FROM test WHERE x = $1;
SELECT get_queryId('EXECUTE stmt1(1)') AS query_id \gset

EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE stmt1(1); -- custom plan
SELECT pg_mentor_set_plan_mode(:query_id, 1);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE stmt1(1); -- generic plan
SELECT pg_mentor_set_plan_mode(:query_id, 2);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE stmt1(1); -- custom plan
SELECT pg_mentor_set_plan_mode(:query_id, 0);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE stmt1(1); -- auto mode

SELECT oid AS dboid FROM pg_database WHERE datname = current_database() \gset

SELECT true FROM pg_stat_statements_reset(0, :dboid);

-- Prepare the case when custom plan cost all the time much less than the
-- generic one
CREATE TABLE part (
	id int
) PARTITION BY RANGE (id);
CREATE TABLE part1 PARTITION OF part FOR VALUES FROM (0) to (100);
CREATE TABLE part2 PARTITION OF part FOR VALUES FROM (100) to (200);
INSERT INTO part (id) SELECT x%200 FROM generate_series(1,1E4) AS x;
CREATE INDEX part_idx_1 ON part (id);
CREATE INDEX part_idx_2 ON part (id);
CREATE INDEX part_idx_3 ON part (id);
VACUUM ANALYZE part,part1,part2;

PREPARE qry (integer) AS SELECT * FROM part WHERE id IN ($1, $1+1);

EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry(1);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry(2);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry(3);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry(4);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry(5);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry(6);

EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry(1); -- it uses custom plan yet
SELECT * FROM reconsider_ps_modes();

EXPLAIN (COSTS OFF) EXECUTE qry(1); -- should be generic plan
SELECT * FROM reconsider_ps_modes(0, true); -- and try again, clear stat at the end.

CREATE TABLE part3 AS SELECT 201::int AS id
FROM generate_series(1,1E4) AS x;
VACUUM ANALYZE part3;
ALTER TABLE part ATTACH PARTITION part3 FOR VALUES FROM (200) to (301);
SELECT pg_mentor_reset(); -- Clear previous decisions & PS-statistics

-- Check reset operation
SELECT
  refcounter, plan_cache_mode, fixed, statnum, nblocks, exec_times,
  avg_nblocks, avg_exec_time, ref_nblocks, ref_exec_time
FROM pg_mentor_show_prepared_statements(-1) ps;

--
-- qry1 is a subject of pruning.
-- qry2 may cause multiple-blocks scanning operations
PREPARE qry1 (integer[]) AS SELECT * FROM part WHERE id = ANY ($1);
PREPARE qry2 (integer) AS SELECT * FROM part WHERE id < $1;

-- Just let them to be executed. Stable executions should led to decisions:
-- qry1 - good candidate to be generic (stable execution, long planning)
-- qry2 - a little volatile load don't allow to force it as generic.
-- They both can't be switched to generic automatically because of partition
-- pruning.
\o /dev/null
EXECUTE qry1(ARRAY[1,2]) \watch i=0 c=10

EXECUTE qry2(110);
EXECUTE qry2(190);
EXECUTE qry2(100);
EXECUTE qry2(180);
EXECUTE qry2(135);
\o

-- See the current state of the prepared statements. Both have a custom plan
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry1(ARRAY[1,2]); -- good execution
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry2(110);

-- qry1 - auto -> force generic plan mode (step 1.)
-- qry2 stay as is
SELECT * FROM reconsider_ps_modes1();

-- Check: qry1 (generic), qry2 (custom).
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry1(ARRAY[1,2]);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry2(110);

-- Stable execution enough to not be forced as custom. Also, it can't be
-- switched to generic by the core because of pruned plan cost.
\o /dev/null
EXECUTE qry2(110+random()*5::integer) \watch i=0 c=10
-- more stable executions for qry1
EXECUTE qry1(ARRAY[1,1]) \watch i=0 c=10
\o

-- Previous executions were stable enough. So, no change in decisions.
SELECT * FROM reconsider_ps_modes1();

EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry1(ARRAY[1,201]); --bad execution
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry2(1); -- custom
\o /dev/null
EXECUTE qry1(ARRAY[1,200]);
EXECUTE qry2(30);
EXECUTE qry2(299);
EXECUTE qry2(2);
EXECUTE qry2(10);
EXECUTE qry2(280);
\o

-- change qry1 and qry2 plan cache modes to forced custom.
SELECT * FROM reconsider_ps_modes1();

EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry1(ARRAY[1,3]);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry2(1);

-- Do a series of executions that have similar execution time to decide that
-- it is stable enough to go generic.
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry2(1); -- Must be custom plan
\o /dev/null
EXECUTE qry2(1) \watch i=0 c=10
EXECUTE qry1(ARRAY[1,3]); \watch i=0 c=3
\o

-- Query execution is stable enough: switch back to generic plan for qry2,
--  don't change qry1 plan cache mode.
SELECT * FROM reconsider_ps_modes1();
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry2(1); -- must be generic
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry1(ARRAY[1,3]); -- must be custom

DEALLOCATE ALL;
DROP TABLE test CASCADE;
