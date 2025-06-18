/*
 */

#include "postgres.h"

#include "common/hashfn.h"
#include "executor/executor.h"
#include "optimizer/planner.h"
#include "storage/ipc.h"
#include "tcop/utility.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/plancache.h"

#include "pg_mentor.h"


typedef struct PSMeteringEntryKey
{
	uint64				queryId;
} PSMeteringEntryKey;

typedef struct PSMeteringEntry
{
	PSMeteringEntryKey key;

	/* Should be set in hash table slot only */
	List			   *plansources;

	/*
	 * Statistics needed for managing prepared statements in auto mode.
	 */

	int64	total_generic_nblocks_read;
	double	total_generic_exectime;
	int		generic_meterings;

	int64	total_custom_nblocks_read;
	double	total_custom_exectime;
	int		custom_meterings;

	double	total_plan_time;
	int		plan_meterings;

	int32	attempts_counter;
} PSMeteringEntry;


#define METERINGS_MIN	(100) /* initial number of stat size to make solutions */
#define METERINGS_MAX	(1000) /* reset stat on this value and start to gather new sample */

/*
 * Shall the extension to gather statistics / check the statement state?
 * The idea of this parameter - it should work like a "throttling" to skip
 * operations sometimes.
 */
//static bool metering_should_happen = true;

static bool	manage_auto_mode = true;

static HTAB *metering_htab = NULL;

/* --- HOOKS --- */
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static void
reset_statement_meterings(PSMeteringEntry *entry)
{
	entry->custom_meterings = 0;
	entry->generic_meterings = 0;
	entry->plan_meterings = 0;

	entry->total_plan_time = 0.;
	entry->total_custom_nblocks_read = 0;
	entry->total_generic_nblocks_read = 0;

	entry->total_custom_exectime = 0.;
	entry->total_generic_exectime = 0.;
}

// CACHEDPLANSOURCE_MAGIC

/*
 * Routines, managing the statements cache (add/remove)
 */

static uint32
metering_key_hash(const void *key, Size keysize)
{
	PSMeteringEntryKey *mkey = (PSMeteringEntryKey *) key;

	Assert(keysize == sizeof(PSMeteringEntryKey));

	return hash_bytes((void *) &mkey->queryId, sizeof(int64));
}

/*
 * Key comparison function.
 *
 * Return zero for match, nonzero for no match.
 */
static int
metering_key_cmp(const void *key1, const void *key2, Size keysize)
{
	return memcmp(key1, key2, keysize);
}

static void
recreate_local_htab()
{
	HASHCTL		ctl;

	if (metering_htab != NULL)
		return;

	ctl.keysize = sizeof(PSMeteringEntryKey);
	ctl.entrysize = sizeof(PSMeteringEntry);
	ctl.match = metering_key_cmp;
	ctl.hash = metering_key_hash;
	metering_htab = hash_create("pg_mentor metering HTAB", 128, &ctl,
								 HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
}

static int nesting_level = 0;

/*
 * Measure planning time
 */
static PlannedStmt *
metering_planner(Query *parse, const char *query_string,
			int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt		   *result;
	PSMeteringEntry	   *entry = NULL;
	instr_time			start;
	instr_time			duration;
	bool				found;

	if (manage_auto_mode && pgm_enabled(nesting_level) && query_string &&
		parse->queryId != INT64CONST(0))
	{
		PSMeteringEntryKey key = {.queryId = parse->queryId};

		recreate_local_htab();

		entry = hash_search(metering_htab, &key, HASH_FIND, &found);
		if (likely(!found))
			INSTR_TIME_SET_ZERO(start);
		else
			INSTR_TIME_SET_CURRENT(start);
	}
	else
		INSTR_TIME_SET_ZERO(start);

	nesting_level++;
	PG_TRY();
	{
		if (prev_planner_hook)
			result = (*prev_planner_hook) (parse, query_string,
										   cursorOptions, boundParams);
		else
			result = standard_planner(parse, query_string, cursorOptions,
									  boundParams);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();

	if (!INSTR_TIME_IS_ZERO(start))
	{
		INSTR_TIME_SET_CURRENT(duration);
		INSTR_TIME_SUBTRACT(duration, start);

		entry->total_plan_time += INSTR_TIME_GET_MILLISEC(duration);
		entry->plan_meterings++;
	}

	return result;
}

void
automode_on_prepare(PreparedStatement  *ps)
{
	uint64 queryId = get_prepared_stmt_queryId(ps);
	PSMeteringEntryKey key = {.queryId = queryId};
	PSMeteringEntry *entry;
	bool found;
	MemoryContext oldctx;

	if (queryId == UINT64CONST(0))
		return;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	entry = hash_search(metering_htab, &key, HASH_ENTER, &found);
	if (!found)
	{
		entry->plansources = list_make1(ps->plansource);
		reset_statement_meterings(entry);
		entry->attempts_counter = 0;
	}
	else
		entry->plansources = lappend(entry->plansources, ps->plansource);

	MemoryContextSwitchTo(oldctx);
}

void
automode_on_deallocate(uint64 queryId, void *plansource_ptr)
{
	if (queryId == UINT64CONST(0))
	{
		hash_destroy(metering_htab);
		metering_htab = NULL;
		recreate_local_htab();
	}
	else
	{
		PSMeteringEntryKey key = {.queryId = queryId};
		PSMeteringEntry *entry;
		bool found;

		entry = (PSMeteringEntry *) hash_search(metering_htab,
										  &key, HASH_FIND, &found);
		Assert(found);

		if (list_length(entry->plansources) == 1)
		{
			(void) hash_search(metering_htab, &key, HASH_REMOVE, NULL);
		}
		else
		{
			Assert(list_member_ptr(entry->plansources, plansource_ptr));
			entry->plansources = list_delete_ptr(entry->plansources, plansource_ptr);
		}
	}
}

#define GENERIC_DOMINATES \
  (avg_generic_nblocks < avg_custom_nblocks * (1 + avg_plan_weight) && avg_generic_exectime < avg_custom_exectime)
#define CUSTOM_DOMINATES \
  (avg_generic_nblocks > avg_custom_nblocks * (1 + avg_plan_weight) && avg_generic_exectime > avg_custom_exectime)

static bool
ps_need_reset(PSMeteringEntry *entry,
			  double avg_custom_cost, double generic_cost)
{
	double avg_generic_nblocks;
	double avg_custom_nblocks;
	double avg_generic_exectime;
	double avg_custom_exectime;
	double avg_plantime;
	double avg_plan_weight;

	avg_generic_nblocks = entry->total_generic_nblocks_read / entry->generic_meterings;
	avg_custom_nblocks = entry->total_custom_nblocks_read / entry->custom_meterings;
	avg_generic_exectime = entry->total_generic_exectime / entry->generic_meterings;
	avg_custom_exectime = entry->total_custom_exectime / entry->custom_meterings;
	avg_plantime = entry->total_plan_time / entry->plan_meterings;
	avg_plan_weight = avg_plantime / avg_custom_exectime;

	if ((generic_cost > avg_custom_cost && GENERIC_DOMINATES) ||
		(generic_cost < avg_custom_cost && CUSTOM_DOMINATES))
	{
		// Costs need to be recalculated. Reset prepared statement statistics
		entry->attempts_counter++;
		return true;
	}

	return false;
}

/*
 * Decide if it is needed to reset current core solution
 */
static void
pgm_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	uint64		queryId = queryDesc->plannedstmt->queryId;
	PSMeteringEntryKey key = {.queryId = queryId};
	PSMeteringEntry *entry;
	bool			found;
	int				nmeterings;

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (!manage_auto_mode || !pgm_enabled(nesting_level) ||
		queryId == UINT64CONST(0) || !((eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0))
		return;

	/* Be gentle and track queries are known as prepared statements */
	entry = (PSMeteringEntry *) hash_search(metering_htab, &key, HASH_FIND, &found);
	if (!found)
		return;

	nmeterings = entry->custom_meterings > 0 && entry->generic_meterings > 0 ?
                        entry->custom_meterings + entry->generic_meterings : 0;

	/* Make a decision in case we have enough statistical measurements */
	if (nmeterings >= METERINGS_MIN &&
		plan_cache_mode != PLAN_CACHE_MODE_FORCE_GENERIC_PLAN &&
		plan_cache_mode != PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN)
	{
		ListCell *lc;

		foreach(lc, entry->plansources)
		{
			CachedPlanSource *ps = (CachedPlanSource *) lfirst(lc);
			double		avg_custom_cost;

			if (ps->cursor_options & CURSOR_OPT_GENERIC_PLAN ||
				ps->cursor_options & CURSOR_OPT_CUSTOM_PLAN ||
				ps->num_custom_plans <= 5 || ps->generic_cost <= 0.)
				continue;

			avg_custom_cost = ps->total_custom_cost / ps->num_custom_plans;

			if (ps_need_reset(entry, avg_custom_cost, ps->generic_cost))
			{
				ps->generic_cost = 0.;
				ps->total_custom_cost = 0.;
				ps->num_custom_plans = 0;
				ps->num_generic_plans = 0;
			}
		}
	}

	if (queryDesc->totaltime == NULL)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
		queryDesc->totaltime =
					InstrAlloc(1, INSTRUMENT_BUFFERS | INSTRUMENT_TIMER, false);
		MemoryContextSwitchTo(oldcxt);
	}
}

static void
pgm_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

static void
pgm_ExecutorFinish(QueryDesc *queryDesc)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

static void
pgm_ExecutorEnd(QueryDesc *queryDesc)
{
	uint64		queryId = queryDesc->plannedstmt->queryId;
	PSMeteringEntryKey key = {.queryId = queryId};
	PSMeteringEntry *entry;
	bool			found;
	int				nmeterings;

	if (!manage_auto_mode || queryId == UINT64CONST(0) ||
		queryDesc->totaltime == NULL || !pgm_enabled(nesting_level) ||
		!((queryDesc->estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0))
		goto end;

	entry = (PSMeteringEntry *) hash_search(metering_htab, &key, HASH_FIND, &found);
	if (!found)
		goto end;

	nmeterings = entry->custom_meterings > 0 && entry->generic_meterings > 0 ?
                        entry->custom_meterings + entry->generic_meterings : 0;

	if (nmeterings >= METERINGS_MAX)
		reset_statement_meterings(entry);

	entry = (PSMeteringEntry *) hash_search(metering_htab, &key, HASH_FIND, &found);

	if (found)
	{
			BufferUsage *bufusage = &queryDesc->totaltime->bufusage;
			double exec_time = queryDesc->totaltime->total * 1000.0;
			int64				nblocks;
			bool				is_generic = false;
			ListCell *lc;

			InstrEndLoop(queryDesc->totaltime);

			nblocks = bufusage->shared_blks_hit + bufusage->shared_blks_read +
						bufusage->local_blks_hit +bufusage->local_blks_read +
						bufusage->temp_blks_read;

			foreach (lc, entry->plansources)
			{
				CachedPlanSource *ps = (CachedPlanSource *) lfirst(lc);
				PlannedStmt *plannedstmt;

				if (ps->gplan == NULL || ps->gplan->stmt_list == NULL)
					continue;

				plannedstmt = linitial_node(PlannedStmt, ps->gplan->stmt_list);
				if (plannedstmt != queryDesc->plannedstmt)
					continue;

				is_generic = true;
				break;
			}

			if (is_generic)
			{
				entry->total_generic_exectime += exec_time;
				entry->total_generic_nblocks_read += nblocks;
				entry->generic_meterings++;
			}
			else
			{
				entry->total_custom_exectime += exec_time;
				entry->total_custom_nblocks_read += nblocks;
				entry->custom_meterings++;
			}
	}

end:
	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
	if (manage_auto_mode)
	{


  }
	}
*/

void
automode_init()
{
	DefineCustomBoolVariable(MODULENAME".manage_auto_mode",
							 "Manage prepared statements in auto mode",
							 NULL,
							 &manage_auto_mode,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL
	);

	if (manage_auto_mode)
	{
		Assert(metering_htab == NULL);
		recreate_local_htab();
	}

	prev_planner_hook = planner_hook;
	planner_hook = metering_planner;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgm_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pgm_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pgm_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgm_ExecutorEnd;
}
