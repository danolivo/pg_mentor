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

DEALLOCATE ALL;
DROP TABLE test CASCADE;
