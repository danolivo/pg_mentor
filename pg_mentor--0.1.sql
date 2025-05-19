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
CREATE FUNCTION pg_mentor_set_plan_mode(queryId bigint, status integer)
RETURNS bool
AS 'MODULE_PATHNAME', 'pg_mentor_set_plan_mode'
LANGUAGE C;

--
-- Returns description of queries that are under control at the moment
--
CREATE FUNCTION pg_mentor_show_managed_queries(IN status integer,
  OUT queryid bigint,
  OUT plan_cache_mode int,
  OUT since TimestampTz)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_mentor_show_managed_queries'
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
	  WHERE max_exec_time < mean_plan_time) AS q(queryid)
    JOIN LATERAL (SELECT pg_mentor_set_plan_mode(q.queryid, 1)) AS q1(result)
    ON (q1.result = TRUE) INTO cnt;
  RETURN cnt;
END;
$$ LANGUAGE plpgsql;