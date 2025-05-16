/* contrib/pg_mentor/pg_mentor--0.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_mentor" to load this file. \quit

--
-- Set plan cache mode for specific prepared statement.
--
-- Statuses:
-- 0 - PLAN_CACHE_MODE_AUTO
-- 1 - PLAN_CACHE_MODE_FORCE_GENERIC_PLAN
-- 2 - PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN
--
-- Returns true in case of successful finish, false otherwise.
--
CREATE FUNCTION set_prepared_statement_status(stmt_name text, status integer)
RETURNS bool
AS 'MODULE_PATHNAME', 'set_prepared_statement_status'
LANGUAGE C;
