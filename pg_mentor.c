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

#include "commands/prepare.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "nodes/execnodes.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

#define MODULENAME	"pg_mentor"

PG_MODULE_MAGIC_EXT(
					.name = MODULENAME,
					.version = PG_VERSION
);

PG_FUNCTION_INFO_V1(pg_mentor_reload_conf);
PG_FUNCTION_INFO_V1(pg_mentor_set_plan_mode);
PG_FUNCTION_INFO_V1(pg_mentor_show_prepared_statements);
PG_FUNCTION_INFO_V1(pg_mentor_reset);

static const char *psfuncname = "pg_prepared_statement";
static Oid		   psfuncoid = 0;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

/*
 * Single flag for all databases?
 */
typedef struct SharedState
{
	LWLock			   *lock;
	pg_atomic_uint64	state_generation;
} SharedState;

#define MENTOR_TBL_ENTRY_FIELDS_NUM	(4)

typedef struct MentorTblEntry
{
	int64		queryid; /* the key */
	uint32		refcounter; /* How much users use this statement? */
	int			plan_cache_mode;
	TimestampTz	since; /* The moment of addition to the table */
} MentorTblEntry;

static SharedState *state = NULL;
static HTAB		   *pgm_hash = NULL;
static HTAB		   *pgm_local_hash = NULL; /* contains statements, prepared in this backend */

static int max_records_num = 1024;

static uint64 local_state_generation = 0; /* 0 - not initialised */

static void on_deallocate(uint64 queryId);

static void
set_plan_cache_mode(PreparedStatement  *entry, int status)
{
	switch (status)
	{
		case 0:
			/* PLAN_CACHE_MODE_AUTO */
			entry->plansource->cursor_options &=
							~(CURSOR_OPT_CUSTOM_PLAN | CURSOR_OPT_GENERIC_PLAN);
			break;
		case 1:
			/* PLAN_CACHE_MODE_FORCE_GENERIC_PLAN */
			entry->plansource->cursor_options &= ~CURSOR_OPT_CUSTOM_PLAN;
			entry->plansource->cursor_options |= CURSOR_OPT_GENERIC_PLAN;
			break;
		case 2:
			/* PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN */
			entry->plansource->cursor_options &= ~CURSOR_OPT_GENERIC_PLAN;
			entry->plansource->cursor_options |= CURSOR_OPT_CUSTOM_PLAN;
			break;
		default:
			Assert(0);
	}
}

/*
 * Call pg_prepared_statement and return list of statement names.
 *
 * Returns list of PreparedStatement pointers
 */
static List *
fetch_prepared_statements(void)
{
	FunctionCallInfo	fcinfo = palloc0(SizeForFunctionCallInfo(0));
	ReturnSetInfo		rsinfo;
	FmgrInfo			ps_fmgr_info;
	List			   *pslst = NIL;
	int64				nvalues;

	/*
	 * Settings to call SRF routine. See InitMaterializedSRF.
	 */
	rsinfo.type = T_ReturnSetInfo;
	rsinfo.econtext = CreateStandaloneExprContext();
	rsinfo.expectedDesc = NULL;
	rsinfo.allowedModes = (int) (SFRM_ValuePerCall | SFRM_Materialize);
	rsinfo.returnMode = SFRM_Materialize;
	rsinfo.setResult = NULL;
	rsinfo.setDesc = NULL;

	fmgr_info(psfuncoid, &ps_fmgr_info);
	InitFunctionCallInfoData(*fcinfo, &ps_fmgr_info, 0, InvalidOid, NULL, NULL);
	fcinfo->resultinfo = (Node *) &rsinfo;

	(void) FunctionCallInvoke(fcinfo);

	nvalues = tuplestore_tuple_count(rsinfo.setResult);

	if (nvalues > 0)
	{
		TupleTableSlot *slot;

		slot = MakeSingleTupleTableSlot(rsinfo.setDesc, &TTSOpsMinimalTuple);
		while (tuplestore_gettupleslot(rsinfo.setResult, true, false, slot))
		{
			char			   *stmt_name;
			bool				isnull;
			PreparedStatement  *ps;

			stmt_name = TextDatumGetCString(slot_getattr(slot, 1, &isnull));
			Assert(!isnull);

			ps = FetchPreparedStatement(stmt_name, false);
			Assert(ps != NULL);
			pslst = lappend(pslst, ps);

			ExecClearTuple(slot);
		}
	}

	return pslst;
}

/*
 * The prepared_queries hash table is private core entity. So let's manage the
 * extension internal hash table. It can help us to cleanup global table on
 * backend exit.
 */
typedef struct LocaLPSEntry
{
	uint64 queryId;
} LocaLPSEntry;

/*
 * Does prepared statements table changed?
 *
 * Prepared statement should be revalidated before deciding on building a plan.
 * At this moment any shift in management table may be detected and new plan
 * options applied.
 *
 * XXX: it seems not ideal solution due to slow down in arbitrary query
 * planning. Is this an architectural defect of Postgres or my lack of
 * understanding? Anyway, without custom invalidation messages it looks like
 * we have no alternatives.
 */
static void
check_state(void)
{
	HASH_SEQ_STATUS hash_seq;
	uint64			generation;
	MentorTblEntry *entry;
	List		   *pslst;

	generation = pg_atomic_read_u64(&state->state_generation);

	if (generation == local_state_generation)
		return;

	pslst = fetch_prepared_statements();

	if (list_length(pslst) == 0)
		return;

	/*
	 * Pass through all the table, match prepared statement with the same
	 * queryId and set up plan type options.
	 */
	LWLockAcquire(state->lock, LW_SHARED);
	hash_seq_init(&hash_seq, pgm_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		ListCell *lc;

		foreach(lc, pslst)
		{
			PreparedStatement *ps = (PreparedStatement *) lfirst(lc);
			Query *query = linitial_node(Query, ps->plansource->query_list);

			if (query->queryId != entry->queryid)
				continue;

			set_plan_cache_mode(ps, entry->plan_cache_mode);
		}
	}
	LWLockRelease(state->lock);

	if (local_state_generation < generation)
		local_state_generation = generation;
}

static bool
move_mentor_status()
{
	pg_atomic_fetch_add_u64(&state->state_generation, 1);
	return true;
}

Datum
pg_mentor_reload_conf(PG_FUNCTION_ARGS)
{
	move_mentor_status();
	PG_RETURN_BOOL(true);
}

Datum
pg_mentor_set_plan_mode(PG_FUNCTION_ARGS)
{
	int64			queryId = PG_GETARG_INT64(0);
	int				status = PG_GETARG_INT32(1);
	bool			found;
	MentorTblEntry *entry;
	bool			result = false;

	if (!LWLockConditionalAcquire(state->lock, LW_EXCLUSIVE))
		PG_RETURN_BOOL(false);

	entry = (MentorTblEntry *) hash_search(pgm_hash, &queryId, HASH_ENTER, &found);
	if (!found || entry->plan_cache_mode != status)
	{
		entry->plan_cache_mode = status;
		result = true;
	}

	LWLockRelease(state->lock);

	/* Tell other backends that they may update their statuses. */
	move_mentor_status();

	PG_RETURN_BOOL(result);
}

Datum
pg_mentor_show_prepared_statements(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	HASH_SEQ_STATUS hash_seq;
	MentorTblEntry *entry;
	Datum		values[MENTOR_TBL_ENTRY_FIELDS_NUM];
	bool		nulls[MENTOR_TBL_ENTRY_FIELDS_NUM] = {0};

	InitMaterializedSRF(fcinfo, 0);

	LWLockAcquire(state->lock, LW_SHARED);
	if (hash_get_num_entries(pgm_hash) <= 0)
	{
		LWLockRelease(state->lock);
		return (Datum) 0;
	}

	hash_seq_init(&hash_seq, pgm_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		values[0] = Int64GetDatumFast(entry->queryid);
		values[1] = UInt64GetDatum(entry->refcounter);
		values[2] = Int32GetDatum(entry->plan_cache_mode);
		values[3] = TimestampTzGetDatum(entry->since);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	LWLockRelease(state->lock);

	return (Datum) 0;
}

/*
 * Clean all decisions has been made
 */
Datum
pg_mentor_reset(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS hash_seq;
	MentorTblEntry *entry;
	int32			counter = 0;

	LWLockAcquire(state->lock, LW_SHARED);

	if (hash_get_num_entries(pgm_hash) <= 0)
	{
		LWLockRelease(state->lock);
		PG_RETURN_INT32(counter);
	}

	hash_seq_init(&hash_seq, pgm_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (entry->plan_cache_mode != 0)
		{
			entry->plan_cache_mode = 0;
			counter++;
		}
	}
	LWLockRelease(state->lock);
	PG_RETURN_INT32(counter);
}

static void
pgm_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(sizeof(SharedState) +
							hash_estimate_size(max_records_num,
											   sizeof(MentorTblEntry)));
	RequestNamedLWLockTranche(MODULENAME, 1);
}

static void
pgm_shmem_startup(void)
{
	HASHCTL		info;
	bool		found;

	state = NULL;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	state = ShmemInitStruct(MODULENAME, sizeof(SharedState), &found);

	if (!found)
	{
		state->lock = &(GetNamedLWLockTranche(MODULENAME))->lock;
		pg_atomic_init_u64(&state->state_generation, 1);
	}

	info.keysize = sizeof(uint64);
	info.entrysize = sizeof(MentorTblEntry);
	pgm_hash = ShmemInitHash(MODULENAME" hash", max_records_num,
							 max_records_num, &info,
							 HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
}

static void
pgm_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate)
{
	/* Call in advance. If something triggers an error we skip further code */
	if (prev_post_parse_analyze_hook)
		(*prev_post_parse_analyze_hook) (pstate, query, jstate);

	check_state();
}

static PlannedStmt *
pgm_planner(Query *parse, const char *query_string,
			int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result;

	if (prev_planner_hook)
		result = (*prev_planner_hook) (parse, query_string, cursorOptions, boundParams);
	else
		result = standard_planner(parse, query_string, cursorOptions, boundParams);

	check_state();

	return result;
}

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

static bool before_shmem_exit_initialised = false;

static void
before_backend_shutdown(int code, Datum arg)
{
	on_deallocate(UINT64CONST(0));
}

static void
recreate_local_htab()
{
	HASHCTL		ctl;

	if (pgm_local_hash != NULL)
		hash_destroy(pgm_local_hash);

	ctl.keysize = sizeof(uint64);
	ctl.entrysize = sizeof(LocaLPSEntry);
	pgm_local_hash = hash_create("pg_mentor PS hash", 128, &ctl,
								 HASH_ELEM | HASH_BLOBS);
}

static uint32
on_prepare(PreparedStatement  *ps)
{
	int64				queryId = get_prepared_stmt_queryId(ps);
	MentorTblEntry	   *entry;
	bool				found;
	uint32				refcounter;

	if (queryId == UINT64CONST(0))
		return -1;

	LWLockAcquire(state->lock, LW_EXCLUSIVE);
	entry = (MentorTblEntry *) hash_search(pgm_hash, &queryId, HASH_ENTER, &found);

	if (found)
		entry->refcounter++;
	else
	{
		/* Initialise new entry */
		entry->refcounter = 1;
		entry->plan_cache_mode = ps->plansource->cursor_options &
							(CURSOR_OPT_CUSTOM_PLAN | CURSOR_OPT_GENERIC_PLAN);
		entry->since = GetCurrentTimestamp();
	}
	refcounter = entry->refcounter;
	LWLockRelease(state->lock);

	/* Don't forget to insert it locally */
	(void) hash_search(pgm_local_hash, &queryId, HASH_ENTER, &found);
	Assert(!found);

	/* Don't trust to big numbers */
	Assert(refcounter < UINT32_MAX - 1);

	if (!before_shmem_exit_initialised)
	{
		before_shmem_exit(before_backend_shutdown, 0);
		before_shmem_exit_initialised = true;
	}

	return refcounter;
}

/*
 * HTAB has been reset globally
 */
static int32
on_global_reset()
{
}

/*
 * Some sort of deallocation is coming.
 *
 * Find the record in global HTAB and decrease refcounter.
 * Remove the record from the local HTAB.
 */
static void
on_deallocate(uint64 queryId)
{
	MentorTblEntry	   *entry;
	bool				found;

	LWLockAcquire(state->lock, LW_EXCLUSIVE);

	if (queryId != UINT64CONST(0))
	{
		entry = (MentorTblEntry *) hash_search(pgm_hash, &queryId,
											   HASH_FIND, &found);
		if (found)
		{
			entry->refcounter--;
			Assert(entry->refcounter < UINT32_MAX - 1);
			(void) hash_search(pgm_local_hash, &queryId, HASH_REMOVE, NULL);
		}
#ifdef USE_ASSERT_CHECKING
		else
		{
			(void) hash_search(pgm_local_hash, &queryId, HASH_FIND, &found);
			Assert(!found);
		}
#endif
	}
	else
	{
		HASH_SEQ_STATUS hash_seq;
		LocaLPSEntry   *data;

		hash_seq_init(&hash_seq, pgm_local_hash);
		while ((data = hash_seq_search(&hash_seq)) != NULL)
		{
			Assert(data->queryId != UINT64CONST(0));

			entry = (MentorTblEntry *) hash_search(pgm_hash, &data->queryId,
												   HASH_FIND, &found);
			if (found)
			{
				entry->refcounter--;
				Assert(entry->refcounter < UINT32_MAX - 1);
			}

			/*
			 * Sometimes we may not find this entry in global HTAB having in the
			 * local one (reset). But still should delete it locally.
			 */
			(void) hash_search(pgm_local_hash, &data->queryId, HASH_REMOVE, NULL);
		}
	}
	LWLockRelease(state->lock);
}

/*
 * Utility hook.
 *
 * Manage PREPARED STATEMENT entries in the global hash table.
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
	Node   *parsetree = pstmt->utilityStmt;
	uint64	queryId = UINT64CONST(0);
	bool	deallocate_all = false;

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
		}
		else
			deallocate_all = true;
	}

	if (prev_ProcessUtility_hook)
		(*prev_ProcessUtility_hook) (pstmt, queryString, readOnlyTree,
									 context, params, queryEnv,
									 dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree,
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

			on_prepare(ps);
		}
			break;
		case T_DeallocateStmt:
		{
			if (queryId != UINT64CONST(0) || deallocate_all)
				on_deallocate(queryId);
		}
			break;
		default:
			break;
	}
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(MODULENAME" extension could be loaded only on startup."),
				 errdetail("Add into the shared_preload_libraries list.")));

	/* Cache oid for further direct calls */
	psfuncoid = fmgr_internal_function(psfuncname);
	Assert(psfuncoid != InvalidOid);

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pgm_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgm_shmem_startup;

	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pgm_post_parse_analyze;
	prev_planner_hook = planner_hook;
	planner_hook = pgm_planner;
	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = pgm_ProcessUtility_hook;

	recreate_local_htab();

	MarkGUCPrefixReserved(MODULENAME);
}
