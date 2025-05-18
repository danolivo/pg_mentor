# Tests for contrib/pgrowlocks

setup {
  CREATE TABLE test(x integer);

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
}

teardown {
    DROP TABLE test;
}

session s1
step s1_prepare { PREPARE stmt1(integer) AS SELECT * FROM test WHERE x = $1; }
step s1_generic { SELECT count(*) FROM get_queryId('EXECUTE stmt1(1)') AS q(query_id), LATERAL (SELECT * FROM pg_mentor_set_plan_mode(q.query_id::bigint, 1)); }
step s1_custom { SELECT count(*) FROM get_queryId('EXECUTE stmt1(1)') AS q(query_id), LATERAL (SELECT * FROM pg_mentor_set_plan_mode(q.query_id::bigint, 2)); }
step s1_show { EXPLAIN (COSTS OFF) EXECUTE stmt1(1); }

session s2
step s2_prepare { PREPARE stmt2(integer) AS SELECT * FROM test WHERE x = $1; }
step s2_show { EXPLAIN (COSTS OFF) EXECUTE stmt2(1); }

permutation s1_prepare s2_prepare s1_generic s2_show s1_show s1_custom s2_show s1_show
