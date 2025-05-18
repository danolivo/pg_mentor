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
#include "nodes/execnodes.h"
#include "miscadmin.h"
#include "parser/analyze.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"

#define MODULENAME	"pg_mentor"

PG_MODULE_MAGIC_EXT(
					.name = MODULENAME,
					.version = PG_VERSION
);

PG_FUNCTION_INFO_V1(pg_mentor_reload_conf);
PG_FUNCTION_INFO_V1(pg_mentor_set_plan_mode);

static const char *psfuncname = "pg_prepared_statement";
static Oid		   psfuncoid = 0;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;

/*
 * Single flag for all databases?
 */
typedef struct SharedState
{
	LWLock			   *lock;
	pg_atomic_uint64	state_generation;
} SharedState;

typedef struct MentorTblEntry
{
	uint64		queryid; /* the key */
	int			plan_cache_mode;
	TimestampTz	since;
} MentorTblEntry;

static SharedState *state = NULL;
static HTAB		   *pgm_hash = NULL;

static int max_records_num = 1024;

static uint64 local_state_generation = 0; /* 0 - not initialised */

static void
set_plan_cache_mode(PreparedStatement  *entry, int status)
{
	switch (status)
	{
		case 0:
			/* PLAN_CACHE_MODE_AUTO */
			if ((entry->plansource->cursor_options &
				(CURSOR_OPT_CUSTOM_PLAN | CURSOR_OPT_GENERIC_PLAN)) == 0)
				elog(WARNING, "The PLAN_CACHE_MODE_AUTO has already been set up");
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
			PreparedStatement   *ps;

			stmt_name = TextDatumGetCString(slot_getattr(slot, 1, &isnull));
			Assert(!isnull);

			ps = FetchPreparedStatement(stmt_name, false);
			if (ps != NULL)
				pslst = lappend(pslst, ps);

			ExecClearTuple(slot);
		}
	}

	return pslst;
}

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
check_state(ParseState *pstate, Query *query, JumbleState *jstate)
{
	HASH_SEQ_STATUS hash_seq;
	uint64			generation;
	MentorTblEntry *entry;
	List		   *pslst;

	/* Call in advance. If something triggers an error we skip further code */
	if (prev_post_parse_analyze_hook)
		(*prev_post_parse_analyze_hook) (pstate, query, jstate);

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

	if (!LWLockConditionalAcquire(state->lock, LW_EXCLUSIVE))
		PG_RETURN_BOOL(false);

	entry = (MentorTblEntry *) hash_search(pgm_hash, &queryId, HASH_ENTER, &found);
	entry->plan_cache_mode = status;
	LWLockRelease(state->lock);

	/* Tell other backends that they may update their statuses. */
	move_mentor_status();

	PG_RETURN_BOOL(true);
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

	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	post_parse_analyze_hook = check_state;

	MarkGUCPrefixReserved(MODULENAME);
}
