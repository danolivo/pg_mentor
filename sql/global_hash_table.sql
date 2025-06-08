CREATE EXTENSION pg_mentor;
CREATE EXTENSION pg_stat_statements;
SELECT 1 AS noname FROM pg_mentor_reset();

-- Show stable (architecture-independent) parameters of the entry and
-- its statistics
CREATE FUNCTION show_entries()
RETURNS TABLE (
  refcounter		integer,
  plan_cache_mode	integer,
  statnum			integer,
  query				text
)  AS $$
BEGIN
  RETURN QUERY
    SELECT p.refcounter,p.plan_cache_mode,p.statnum,s.query
    FROM pg_mentor_show_prepared_statements(-1) p JOIN
	  pg_stat_statements s USING (queryid);
END;
$$ LANGUAGE PLPGSQL;

-- Should show nothing, just check columns nomenclature itself.
-- XXX: parallel tests may interfere here.
SELECT * FROM show_entries() ORDER BY query COLLATE "C";
SELECT * FROM pg_mentor_show_prepared_statements(-1);

-- Dummy test on redundant deallocation
PREPARE stmt0(int) AS SELECT $1+random() AS x;
PREPARE stmt1(int,int) AS SELECT $1+$2 AS x;
PREPARE stmt2(int,int) AS SELECT sqrt($1+$2) AS x;
-- Use \gset to hide the result
EXECUTE stmt0(1) \gset
EXECUTE stmt1(1,2) \gset
EXECUTE stmt2(3,4) \gset
SELECT * FROM show_entries() ORDER BY query COLLATE "C";

DEALLOCATE stmt0;
SELECT * FROM show_entries() ORDER BY query COLLATE "C";
DEALLOCATE stmt0;
SELECT * FROM show_entries() ORDER BY query COLLATE "C";

DEALLOCATE ALL;
SELECT * FROM show_entries() ORDER BY query COLLATE "C";
DEALLOCATE ALL;
SELECT * FROM show_entries() ORDER BY query COLLATE "C";

-- Prepare statements before the exit
PREPARE stmt1(int,int) AS SELECT $1+$2;
PREPARE stmt2(int,int) AS SELECT sqrt($1+$2);
-- see couple of used entries
SELECT * FROM show_entries() ORDER BY query COLLATE "C";

SELECT current_database() AS dbname \gset

-- Let's open a new connection and see what we can find there
\c :dbname
-- exiting, backend cleans its refcounters - all the entries should be zeroed
SELECT * FROM show_entries() ORDER BY query COLLATE "C";
PREPARE stmt0(int) AS SELECT $1+random() AS x;
-- Check that the entry will be reused
SELECT * FROM show_entries() ORDER BY query COLLATE "C";

-- Basically, we want to see how ring buffer works. In this example we don't
-- have any reason to touch disk blocks during planning or execution phases.
-- But it is still not a 100% guarantee of stability.
SELECT p.refcounter,p.plan_cache_mode,p.statnum,p.nblocks,s.query
FROM pg_mentor_show_prepared_statements(-1) p
JOIN pg_stat_statements s USING (queryid) WHERE s.query LIKE '%+random()%';
EXECUTE stmt0(1) \gset
SELECT p.refcounter,p.plan_cache_mode,p.statnum,p.nblocks,s.query
FROM pg_mentor_show_prepared_statements(-1) p
JOIN pg_stat_statements s USING (queryid) WHERE s.query LIKE '%\+random()%';
 \o /dev/null
EXECUTE stmt0(1) \watch i=0 c=100
\o
SELECT p.refcounter,p.plan_cache_mode,p.statnum,p.nblocks,s.query
FROM pg_mentor_show_prepared_statements(-1) p
JOIN pg_stat_statements s USING (queryid) WHERE s.query LIKE '%\+random()%';

-- Warm-up planner caches
SELECT oid FROM pg_class WHERE oid = 2966;
-- Not sure how stable it is, but seems pretty good if nothing in index scan
-- logic will be changed.
PREPARE stmt1(Oid) AS SELECT oid FROM pg_class WHERE oid = $1;
 \o /dev/null
EXECUTE stmt1(2966) \watch i=0 c=5
\o
SELECT p.refcounter,p.plan_cache_mode,p.statnum,p.nblocks,s.query
FROM pg_mentor_show_prepared_statements(-1) p
JOIN pg_stat_statements s USING (queryid) WHERE s.query LIKE '%pg_class%';


DEALLOCATE ALL;
DROP FUNCTION show_entries();
DROP EXTENSION pg_mentor;
DROP EXTENSION pg_stat_statements;
