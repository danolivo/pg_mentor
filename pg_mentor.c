/*-------------------------------------------------------------------------
 *
 * pg_mentor.c
 *		Attempts to tune query settings based on execution statistics.
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_mentor/pg_mentor.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/parallel.h"
#include "access/xact.h"
#include "commands/extension.h"
#include "commands/prepare.h"
#include "common/hashfn.h"
#include "common/pg_prng.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "optimizer/planner.h"
#include "nodes/queryjumble.h"
#include "storage/lwlock.h"
#include "tcop/utility.h"
#include "utils/guc.h"


#define MODULENAME	"pg_mentor"

PG_MODULE_MAGIC_EXT(
					.name = MODULENAME,
					.version = "0.1"
);

#define pgm_enabled(level, queryId) \
	(manage_auto_mode && metering_should_happen && \
	(queryId != UINT64CONST(0)) && !IsParallelWorker() && (level) == 0)

typedef struct PSMeteringEntryKey
{
	uint64				queryId;
} PSMeteringEntryKey;

typedef struct PSMeteringEntry
{
	PSMeteringEntryKey key;

	/* Should be set in hash table slot only */
	List   *plansources;

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


#define METERINGS_MIN	(10) /* initial number of stat size to make solutions */
#define METERINGS_MAX	(1000) /* reset stat on this value and start to gather new sample */


/* Save here all the data that doesn't fit the HTAB. Reset on DEALLOCATE ALL! */
static MemoryContext	HTABExtraMemory = NULL;

/*
 * Shall the extension to gather statistics / check the statement state?
 * The idea of this parameter - it should work like a "throttling" to skip
 * operations sometimes.
 */
static int metering_should_happen = -1;

static bool	manage_auto_mode = true;
static double metering_throttling_factor = 1.;

static HTAB *metering_htab = NULL;

/*
 * List of employed hooks.
 *
 */
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static void automode_on_prepare(PreparedStatement  *ps);
static void automode_on_deallocate(uint64 queryId, void *plansource_ptr);

static uint64
get_prepared_stmt_queryId(PreparedStatement  *ps)
{
	ListCell		   *lc;

	/* To follow this logic check postgres.c and 933848d */
	foreach(lc, ps->plansource->query_list)
	{
		Query	   *query = lfirst_node(Query, lc);

		if (query->queryId == UINT64CONST(0))
			continue;
		return query->queryId;

	}
	return UINT64CONST(0);
}
/*
#include "math.h"

static double
calculateStandardDeviation(int N, int64 data[])
{
    double	sum = 0;
	double	mean;
	double	values = 0;

    for (int i = 0; i < N; i++)
	{
        sum += data[i];
    }

    mean = sum / N;
    for (int i = 0; i < N; i++)
	{
        values += pow(data[i] - mean, 2);
    }

    return sqrt(values / N);
}
*/
static void
call_process_utility_chain(PlannedStmt *pstmt, const char *queryString,
						bool readOnlyTree, ProcessUtilityContext context,
						ParamListInfo params, QueryEnvironment *queryEnv,
						DestReceiver *dest, QueryCompletion *qc)
{
	if (prev_ProcessUtility_hook)
		(*prev_ProcessUtility_hook) (pstmt, queryString, readOnlyTree,
									 context, params, queryEnv,
									 dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv,
								dest, qc);
}

/*
 * Utility hook.
 *
 * Manage PREPARED STATEMENT entries.
 *
 * At the end of PREPARE or DEALLOCATE statement add queryId of the
 * statement into the global hash table. In case of deallocation just reduce
 * refcounter and let it exist in the table for much longer.
 *
 * Supply it with timestamp to let future clean procedure know how old is
 * this entry.
 *
 * It is not all the add/remove machinery because prepared statement refcounter
 * may be reduced in case of died process or else accidents (need to be
 * discovered). So, we also need manual cleaner to remove old/unused/unmanaged
 * entries from the table.
 */
static void
pgm_ProcessUtility_hook(PlannedStmt *pstmt, const char *queryString,
						bool readOnlyTree, ProcessUtilityContext context,
						ParamListInfo params, QueryEnvironment *queryEnv,
						DestReceiver *dest, QueryCompletion *qc)
{
	Node	   *parsetree = pstmt->utilityStmt;
	uint64		queryId = UINT64CONST(0);
	void	   *plansource_ptr;
	bool		deallocate_all = false;

	if (!IsTransactionState() || !get_extension_oid(MODULENAME, true))
	{
		/*
		 * Our extension doesn't exist in the database the backend is
		 * registered in, do nothing.
		 */
		call_process_utility_chain(pstmt, queryString, readOnlyTree,
								   context, params, queryEnv,
								   dest, qc);
		return;
	}

	/*
	 * Need to save queryId in advance, because deallocate operation removes
	 * the entry from the prepared statements hash table.
	 */
	if (IsA(parsetree, DeallocateStmt))
	{
		DeallocateStmt	   *stmt = (DeallocateStmt *) parsetree;

		if (stmt->name != NULL)
		{
			PreparedStatement  *ps = FetchPreparedStatement(stmt->name, false);

			queryId = (ps == NULL) ?	UINT64CONST(0) :
										get_prepared_stmt_queryId(ps);
			plansource_ptr = (ps == NULL) ? NULL : ps->plansource;
		}
		else
			deallocate_all = true;
	}

	/* Let the core to execute command before the further operations */
	call_process_utility_chain(pstmt, queryString, readOnlyTree,
							   context, params, queryEnv,
							   dest, qc);

	/*
	 * Now operation is finished successfully and we may do the job. Use
	 * the same terminology as the standard_ProcessUtility does.
	 */

	switch (nodeTag(parsetree))
	{
		case T_PrepareStmt:
		{
			PrepareStmt		   *stmt = (PrepareStmt *) parsetree;
			PreparedStatement  *ps = FetchPreparedStatement(stmt->name, true);

			automode_on_prepare(ps);
		}
			break;
		case T_DeallocateStmt:
		{
			if (queryId != UINT64CONST(0) || deallocate_all)
				automode_on_deallocate(queryId, plansource_ptr);
		}
			break;
		default:
			break;
	}
}

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

	/* Roll a die */
	if (pg_prng_double(&pg_global_prng_state) > metering_throttling_factor)
		metering_should_happen = 0;
	else
		metering_should_happen = 1;

	if (pgm_enabled(nesting_level, parse->queryId) && query_string)
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

		/* Measure planning time and keep track of the average value */
		entry->total_plan_time += INSTR_TIME_GET_MILLISEC(duration);
		entry->plan_meterings++;
	}

	return result;
}

/*
 * Create an entry, if have not existed before
 *
 * Alocate all out-of-the-htab memory in the HTABExtraMemory. Don't forget
 * to free it later, during deallocation.
 */
static void
automode_on_prepare(PreparedStatement  *ps)
{
	uint64				queryId = get_prepared_stmt_queryId(ps);
	PSMeteringEntryKey	key = {.queryId = queryId};
	PSMeteringEntry	   *entry;
	bool				found;
	MemoryContext		oldctx;

	if (queryId == UINT64CONST(0))
		return;

	oldctx = MemoryContextSwitchTo(HTABExtraMemory);
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

static void
automode_on_deallocate(uint64 queryId, void *plansource_ptr)
{
	if (queryId == UINT64CONST(0))
	{
		hash_destroy(metering_htab);
		MemoryContextReset(HTABExtraMemory);
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
			list_free(entry->plansources);
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
  (avg_generic_nblocks < avg_custom_nblocks * (1 + avg_plan_weight) && \
  avg_generic_exectime < avg_custom_exectime)
#define CUSTOM_DOMINATES \
  (avg_generic_nblocks > avg_custom_nblocks * (1 + avg_plan_weight) && \
  avg_generic_exectime > avg_custom_exectime)

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
metering_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	uint64		queryId = queryDesc->plannedstmt->queryId;
	PSMeteringEntryKey key = {.queryId = queryId};
	PSMeteringEntry *entry;
	bool			found;
	int				nmeterings;

	/* Roll a die, if planning step has been skipped for a reason */
	if (metering_should_happen < 0.)
	{
		if (pg_prng_double(&pg_global_prng_state) > metering_throttling_factor)
			metering_should_happen = 0;
		else
			metering_should_happen = 1;
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (!pgm_enabled(nesting_level, queryId) ||
		!((eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0))
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
metering_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
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
metering_ExecutorFinish(QueryDesc *queryDesc)
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
metering_ExecutorEnd(QueryDesc *queryDesc)
{
	uint64				queryId = queryDesc->plannedstmt->queryId;
	PSMeteringEntryKey	key = {.queryId = queryId};
	PSMeteringEntry	   *entry;
	bool				found;
	int					nmeterings;

	if (!pgm_enabled(nesting_level, queryId) || queryDesc->totaltime == NULL ||
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
		BufferUsage	   *bufusage = &queryDesc->totaltime->bufusage;
			double			exec_time = queryDesc->totaltime->total * 1000.0;
		int64			nblocks;
		bool			is_generic = false;
		ListCell	   *lc;

		InstrEndLoop(queryDesc->totaltime);

		nblocks = bufusage->shared_blks_hit + bufusage->shared_blks_read +
						bufusage->local_blks_hit +bufusage->local_blks_read +
						bufusage->temp_blks_read;

		foreach (lc, entry->plansources)
		{
			CachedPlanSource   *ps = (CachedPlanSource *) lfirst(lc);
			PlannedStmt		   *plannedstmt;

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

	metering_should_happen = -1;
}

void
_PG_init(void)
{
	EnableQueryId();

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

	DefineCustomRealVariable(MODULENAME ".throttling_factor",
							"Fraction of executions that will participate in meterings",
							"Kind of throttling, needed to reduce slowdowns",
							&metering_throttling_factor,
							1.0,
							.0,
							1.,
							PGC_USERSET,
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
	ExecutorStart_hook = metering_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = metering_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = metering_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = metering_ExecutorEnd;
	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = pgm_ProcessUtility_hook;

	HTABExtraMemory = AllocSetContextCreate(CacheMemoryContext,
											 MODULENAME" - HTABExtraMemory",
											 ALLOCSET_DEFAULT_SIZES);

	MarkGUCPrefixReserved(MODULENAME);
}
