/* contrib/pg_mentor/pg_mentor--0.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_mentor" to load this file. \quit

--
-- Call to update state of prepared statements at each backend.
--
-- This routine just signal to do it - backend checks this flag and do something
-- during a parse tree analyse. So, if it does nothing, actual state will not be
-- updated until any query will come.
--
-- Returns true in case of successful finish, false otherwise.
--
--
CREATE FUNCTION pg_mentor_reload_conf(void)
RETURNS bool
AS 'MODULE_PATHNAME', 'pg_mentor_reload_conf'
LANGUAGE C;

--
-- Set plan cache mode for prepared statements with specific queryId.
--
-- Modes:
-- 0 - use auto mode
-- 1 - force generic plan
-- 2 - force custom plan
--
-- If someone holds the state of the extension it doesn't wait for the end of
-- operation and returns false. So, check the return value and re-call it again
-- if necessary.
--
CREATE FUNCTION pg_mentor_set_plan_mode(queryId bigint,
										status integer,
										ref_exec_time float8 DEFAULT -1)
RETURNS bool
AS 'MODULE_PATHNAME', 'pg_mentor_set_plan_mode'
LANGUAGE C;

--
-- Returns description of queries that are under control at the moment
-- status: -1 = return all the statements; 0 - in the "AUTO" mode;
-- 1 - forced to build generic plan; 2 - forced to build custom plan.
--
CREATE FUNCTION pg_mentor_show_prepared_statements(
  IN status integer,
  OUT queryid bigint,
  OUT refcounter integer,
  OUT plan_cache_mode int,
  OUT since TimestampTz,
  OUT ref_exec_time	float8)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_mentor_show_prepared_statements'
LANGUAGE C;

CREATE FUNCTION pg_mentor_reset()
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_mentor_reset'
LANGUAGE C;

--
-- Demo routine:
-- Implements strategy that detect queries which have planning time much more
-- than max execution time and force generic plan on them.
--
CREATE FUNCTION pg_mentor_nail_long_planned()
RETURNS integer AS $$
DECLARE
  cnt integer;
BEGIN
  SELECT count(*) FROM (
    SELECT queryid::bigint FROM pg_stat_statements
	WHERE queryid IN (SELECT queryid FROM pg_mentor_show_prepared_statements(-1)) AND max_exec_time < mean_plan_time) AS q(queryid)
    JOIN LATERAL (SELECT pg_mentor_set_plan_mode(q.queryid, 1)) AS q1(result)
    ON (q1.result = TRUE) INTO cnt;
  RETURN cnt;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION reconsider_ps_modes(IN ncalls integer DEFAULT 0,
									IN reset_stat boolean DEFAULT false,
									OUT to_generic bigint,
									OUT to_custom bigint,
									OUT unchanged bigint)
RETURNS record AS $$
DECLARE
  dboid		Oid;
  result	record;
BEGIN
  WITH candidates_1 AS (
    -- 1. probe non-extension-forced plans looking good to be generic
    SELECT queryid, total_exec_time AS tet
    FROM pg_stat_statements ss JOIN pg_mentor_show_prepared_statements(-1) ps
	USING (queryid)
	WHERE calls > 1 AND
	  calls > ncalls AND ref_exec_time IS NULL AND
	  ps.plan_cache_mode < 1 AND total_exec_time <= total_plan_time * 2.0 AND
	  total_exec_time > 0.0 AND
	  (max_exec_time-min_exec_time)/mean_exec_time <= 2.0
  ), candidates_2 AS (
    -- 2. detect unsuccessful 'to generic' switches
    SELECT queryid, total_exec_time AS tet
    FROM pg_stat_statements ss JOIN pg_mentor_show_prepared_statements(-1) ps
	USING (queryid)
	WHERE
      -- Basic filters
      calls > ncalls AND total_exec_time > total_plan_time * 2.0 AND
      -- The action 2 filters
      ps.plan_cache_mode = 1 AND
      ref_exec_time > 0.0 AND total_exec_time/ref_exec_time > 2.0
  ), candidates_3 AS (
	-- 3. probe non-extension-forced plans looking good to be custom
	SELECT queryid, total_exec_time AS tet
    FROM pg_stat_statements ss JOIN pg_mentor_show_prepared_statements(-1) ps
	USING (queryid)
	WHERE calls > 1 AND
      calls > ncalls AND ref_exec_time IS NULL AND
	  ps.plan_cache_mode < 1 AND total_exec_time > total_plan_time * 2.0 AND
	  (max_exec_time-min_exec_time)/mean_exec_time > 2.0
  )
  -- Switch query plan mode in the global hash table
  SELECT q1.to_generic, q2.to_custom, q3.unchanged FROM (
    (SELECT count(*) AS to_generic FROM
      (SELECT pg_mentor_set_plan_mode(queryid, 1, tet) FROM candidates_1)
    ) q1 JOIN
    (SELECT count(*) AS to_custom FROM (
	  SELECT pg_mentor_set_plan_mode(queryid, 2, tet) FROM candidates_2
	    UNION ALL
	  SELECT pg_mentor_set_plan_mode(queryid, 2, tet) FROM candidates_3)
    ) q2 JOIN LATERAL
    (SELECT x - q2.to_custom - q1.to_generic AS unchanged FROM
      (SELECT count(*) AS x FROM pg_mentor_show_prepared_statements(-1))) q3
    ON true ON true) INTO result;

  to_generic := result.to_generic;
  to_custom := result.to_custom;
  unchanged := result.unchanged;

  -- Cleanup statistics related to current database, if requested.
  IF (reset_stat IS TRUE) THEN
    SELECT oid AS dboid FROM pg_database
	WHERE datname = current_database() INTO dboid;
	PERFORM pg_stat_statements_reset(0, dboid);
  END IF;
END;
$$ LANGUAGE plpgsql;
