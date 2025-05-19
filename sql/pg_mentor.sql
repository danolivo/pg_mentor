CREATE EXTENSION pg_mentor CASCADE;

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

EXPLAIN (COSTS OFF) EXECUTE stmt1(1); -- custom plan
SELECT pg_mentor_set_plan_mode(:query_id, 1);
EXPLAIN (COSTS OFF) EXECUTE stmt1(1); -- generic plan
SELECT pg_mentor_set_plan_mode(:query_id, 2);
EXPLAIN (COSTS OFF) EXECUTE stmt1(1); -- custom plan
SELECT pg_mentor_set_plan_mode(:query_id, 0);
EXPLAIN (COSTS OFF) EXECUTE stmt1(1); -- auto mode

-- Prepare the case when custom plan cost all the time much less than the
-- generic one
SELECT count(*) FROM pg_stat_statements_reset();
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
EXECUTE qry(1);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry(1);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry(1);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry(1);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE qry(1);

EXPLAIN (COSTS OFF) EXECUTE qry(1); -- it uses custom plan yet
SELECT pg_mentor_nail_long_planned();
EXPLAIN (COSTS OFF) EXECUTE qry(1); -- should be generic plan

SELECT query, plan_cache_mode, calls
FROM pg_stat_statements pgss, pg_mentor_show_managed_queries(-1) pgmq
WHERE pgss.queryid = pgmq.queryid
ORDER BY md5(query);

DEALLOCATE ALL;
DROP TABLE test CASCADE;
