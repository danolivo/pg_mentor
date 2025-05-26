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
    OUT to_generic integer,
    OUT to_custom integer,
    OUT unchanged integer
)
RETURNS record AS $$
BEGIN
--
-- Action No.1:
-- Probe custom, not yet switched plans.
--
  SELECT count(*) FROM (
    WITH candidates AS (
      SELECT
        queryid, min_exec_time AS mit,
		max_exec_time AS met,
		mean_exec_time AS aet,
	    mean_plan_time AS pt
      FROM pg_stat_statements ss JOIN pg_mentor_show_prepared_statements(-1)
	  USING (queryid)
	  WHERE
	    ref_exec_time IS NULL AND calls > ncalls AND mean_exec_time > 0.0 AND
	    ((max_exec_time-min_exec_time)/mean_exec_time < 2.0 OR
											total_exec_time < total_plan_time)
    )
    SELECT pg_mentor_set_plan_mode(candidates.queryid, 1, met) FROM candidates
  ) INTO to_generic;

--
-- Action No.2:
--

  SELECT count(*) - to_generic FROM pg_mentor_show_prepared_statements(-1)
  INTO unchanged;
END;
$$ LANGUAGE plpgsql;
