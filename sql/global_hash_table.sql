CREATE EXTENSION pg_mentor CASCADE;
SELECT 1 AS noname FROM pg_mentor_reset();

CREATE FUNCTION show_entries()
RETURNS TABLE (
  refcounter		integer,
  plan_cache_mode	integer,
  query				text
)  AS $$
BEGIN
  RETURN QUERY
    SELECT p.refcounter,p.plan_cache_mode,s.query
    FROM pg_mentor_show_prepared_statements(-1) p JOIN
	  pg_stat_statements s USING (queryid);
END;
$$ LANGUAGE PLPGSQL;

-- Should show nothing. XXX: parallel tests may interfere here.
SELECT * FROM show_entries() ORDER BY md5(query);

-- Dummy test on redundant deallocation
PREPARE stmt0(int) AS SELECT $1+random() AS x;
PREPARE stmt1(int,int) AS SELECT $1+$2 AS x;
PREPARE stmt2(int,int) AS SELECT sqrt($1+$2) AS x;
-- Use \gset to hide the result
EXECUTE stmt0(1) \gset
EXECUTE stmt1(1,2) \gset
EXECUTE stmt2(3,4) \gset
SELECT * FROM show_entries() ORDER BY md5(query);

DEALLOCATE stmt0;
SELECT * FROM show_entries() ORDER BY md5(query);
DEALLOCATE stmt0;
SELECT * FROM show_entries() ORDER BY md5(query);

DEALLOCATE ALL;
SELECT * FROM show_entries() ORDER BY md5(query);
DEALLOCATE ALL;
SELECT * FROM show_entries() ORDER BY md5(query);

-- Prepare statements before the exit
PREPARE stmt1(int,int) AS SELECT $1+$2;
PREPARE stmt2(int,int) AS SELECT sqrt($1+$2);
-- see couple of used entries
SELECT * FROM show_entries() ORDER BY md5(query);

SELECT current_database() AS dbname \gset

-- Let's open a new connection and see what we can find there
\c :dbname
-- exiting, backend cleans its refcounters - all the entries should be zeroed
SELECT * FROM show_entries() ORDER BY md5(query);
PREPARE stmt0(int) AS SELECT $1+random();
-- Check that the entry will be reused
SELECT * FROM show_entries() ORDER BY md5(query);

DEALLOCATE ALL;
DROP FUNCTION show_entries();
DROP EXTENSION pg_mentor;
