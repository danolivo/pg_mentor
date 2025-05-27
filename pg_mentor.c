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

#include "access/xact.h"
#include "commands/extension.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "lib/dshash.h"
#include "nodes/execnodes.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "storage/dsm_registry.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
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

/*
 * List of intercepted hooks.
 *
 * Each hook should cehck existence of the extension. In case it doesn't exist
 * it should detach from shared structures, if existed.
 */
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

/*
 * Single flag for all databases?
 *
 * There are two global flags exists:
 * 1. Decisions - each time we make a decision to switch some plans to another
 * state we should signal backends to re-read the state.
 * 2. Prepared Statements table - if we are not sure about consistency we may
 * reset whole table of prepared statements - in this case each backend will
 * need to re-read its prepared statements and report them to the global state.
 */
typedef struct SharedState
{
	int					tranche_id;
	pg_atomic_uint64	state_decisions;

	dsa_handle			dsah;
	dshash_table_handle	dshh;

	/* Just for DEBUG */
	Oid					dbOid;
} SharedState;

#define MENTOR_TBL_ENTRY_FIELDS_NUM	(6)

typedef struct MentorTblEntry
{
	uint64		queryid; /* the key */
	uint32		refcounter; /* How much users use this statement? */
	int			plan_cache_mode;
	TimestampTz	since; /* The moment of addition to the table */

	double		ref_exec_time; /* execution time before the switch (or -1) */
	bool		fixed; /* May it be changed automatically? */
} MentorTblEntry;

static dsa_area *dsa = NULL;

static dshash_parameters dsh_params = {
	sizeof(uint64),
	sizeof(MentorTblEntry),
	dshash_memcmp,
	dshash_memhash,
	dshash_memcpy,
	-1
};

static SharedState *state = NULL;
static dshash_table *pgm_hash = NULL;
static HTAB		   *pgm_local_hash = NULL; /* contains statements, prepared in this backend */

static uint64 local_state_generation = 0; /* 0 - not initialised */

static void on_deallocate(uint64 queryId);
static bool pgm_init_shmem(void);

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

static int
get_plan_cache_mode(PreparedStatement *ps)
{
	if (ps->plansource->cursor_options &
							~(CURSOR_OPT_CUSTOM_PLAN | CURSOR_OPT_GENERIC_PLAN))
		return 0; /* PLAN_CACHE_MODE_AUTO */
	if (ps->plansource->cursor_options & CURSOR_OPT_GENERIC_PLAN)
		return 1; /* PLAN_CACHE_MODE_FORCE_GENERIC_PLAN */
	if (ps->plansource->cursor_options & CURSOR_OPT_CUSTOM_PLAN)
		return 2; /* PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN */

	Assert(0);
	return -1;
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
	uint64	queryId;
	int32	refcounter;
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
	dshash_seq_status hash_seq;
	uint64			generation;
	MentorTblEntry *entry;
	List		   *pslst;

	generation = pg_atomic_read_u64(&state->state_decisions);

	if (generation == local_state_generation)
		return;

	pslst = fetch_prepared_statements();

	if (list_length(pslst) == 0)
		return;

	/*
	 * Pass through all the table, match prepared statement with the same
	 * queryId and set up plan type options.
	 */
	dshash_seq_init(&hash_seq, pgm_hash, false);
	while ((entry = dshash_seq_next(&hash_seq)) != NULL)
	{
		ListCell *lc;

		Assert(state->dbOid == MyDatabaseId);

		foreach(lc, pslst)
		{
			PreparedStatement *ps = (PreparedStatement *) lfirst(lc);
			Query			  *query;

			query = linitial_node(Query, ps->plansource->query_list);

			if (query->queryId != entry->queryid)
				continue;

			set_plan_cache_mode(ps, entry->plan_cache_mode);
		}
	}
	dshash_seq_term(&hash_seq);

	if (local_state_generation < generation)
		local_state_generation = generation;
}

static bool
move_mentor_status()
{
	pg_atomic_fetch_add_u64(&state->state_decisions, 1);
	return true;
}

Datum
pg_mentor_reload_conf(PG_FUNCTION_ARGS)
{
	pgm_init_shmem();

	move_mentor_status();
	PG_RETURN_BOOL(true);
}

Datum
pg_mentor_set_plan_mode(PG_FUNCTION_ARGS)
{
	int64			queryId = PG_GETARG_INT64(0);
	int				status = PG_GETARG_INT32(1);
	double			ref_exec_time = PG_GETARG_FLOAT8(2);
	bool			fixed = PG_GETARG_BOOL(3);
	bool			found;
	MentorTblEntry *entry;
	bool			result = false;

	pgm_init_shmem();

	entry = (MentorTblEntry *) dshash_find_or_insert(pgm_hash, &queryId, &found);
	entry->plan_cache_mode = status;
	entry->ref_exec_time = ref_exec_time;
	entry->fixed = fixed;
	result = true;

	dshash_release_lock(pgm_hash, entry);
	/* Tell other backends that they may update their statuses. */
	move_mentor_status();

	PG_RETURN_BOOL(result);
}

Datum
pg_mentor_show_prepared_statements(PG_FUNCTION_ARGS)
{
	int					status = PG_GETARG_INT32(0);
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	dshash_seq_status	hash_seq;
	MentorTblEntry	   *entry;

	pgm_init_shmem();

	InitMaterializedSRF(fcinfo, 0);

	dshash_seq_init(&hash_seq, pgm_hash, false);
	while ((entry = dshash_seq_next(&hash_seq)) != NULL)
	{
		Datum	values[MENTOR_TBL_ENTRY_FIELDS_NUM] = {0};
		bool	nulls[MENTOR_TBL_ENTRY_FIELDS_NUM] = {0};

		/* Do we need to skip this record? */
		if (status >= 0 && status != entry->plan_cache_mode)
			continue;

		values[0] = Int64GetDatumFast((int64) entry->queryid);
		values[1] = UInt64GetDatum(entry->refcounter);
		values[2] = Int32GetDatum(entry->plan_cache_mode);
		values[3] = TimestampTzGetDatum(entry->since);
		if (entry->ref_exec_time >= 0.0)
			values[4] = Float8GetDatum(entry->ref_exec_time);
		else
			nulls[4] = true;
		values[5] = BoolGetDatum(entry->fixed);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	dshash_seq_term(&hash_seq);

	return (Datum) 0;
}

/*
 * Clean all decisions has been made
 */
Datum
pg_mentor_reset(PG_FUNCTION_ARGS)
{
	dshash_seq_status	hash_seq;
	MentorTblEntry	   *entry;
	int32				counter = 0;

	pgm_init_shmem();

	dshash_seq_init(&hash_seq, pgm_hash, true);
	while ((entry = dshash_seq_next(&hash_seq)) != NULL)
	{
		if (entry->plan_cache_mode != 0)
		{
			entry->plan_cache_mode = 0;
			counter++;
		}
	}
	dshash_seq_term(&hash_seq);
	PG_RETURN_INT32(counter);
}

static void
pgm_init_state(void *ptr)
{
	SharedState *state = (SharedState *) ptr;

	state->tranche_id = LWLockNewTrancheId();
	pg_atomic_init_u64(&state->state_decisions, 1);
	state->dbOid = MyDatabaseId;
	Assert(OidIsValid(state->dbOid));

	dsa = dsa_create(state->tranche_id);
	dsa_pin(dsa);
	dsa_pin_mapping(dsa);
	dsh_params.tranche_id = state->tranche_id;
	pgm_hash = dshash_create(dsa, &dsh_params, NULL);

	/* Store handles in shared memory for other backends to use. */
	state->dsah = dsa_get_handle(dsa);
	state->dshh = dshash_get_hash_table_handle(pgm_hash);
}

/*
 * Init database-related shared memory segment.
 *
 * It should be called at the top of each hook or exported function.
 */
static bool
pgm_init_shmem(void)
{
	bool			found;
	char		   *segment_name;
	MemoryContext	memctx;

	if (state != NULL)
		return true;

	Assert(OidIsValid(MyDatabaseId));

	memctx = MemoryContextSwitchTo(TopMemoryContext);
	segment_name = psprintf(MODULENAME"-%u", MyDatabaseId);
	state = GetNamedDSMSegment(segment_name, sizeof(SharedState),
							   pgm_init_state, &found);

	if (found)
	{
		/* Attach to proper database */
		Assert(state->dbOid == MyDatabaseId);

		dsa = dsa_attach(state->dsah);
		dsa_pin_mapping(dsa);
		pgm_hash = dshash_attach(dsa, &dsh_params, state->dshh, NULL);
	}
	LWLockRegisterTranche(state->tranche_id, segment_name);

	MemoryContextSwitchTo(memctx);
	Assert(dsa != NULL && pgm_hash != NULL);
	return found;
}

static void
pgm_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate)
{
	/* Call in advance. If something triggers an error we skip further code */
	if (prev_post_parse_analyze_hook)
		(*prev_post_parse_analyze_hook) (pstate, query, jstate);

	if (!IsTransactionState() || !get_extension_oid(MODULENAME, true))
		/*
		 * Our extension doesn't exist in the database the backend is
		 * registered in, do nothing.
		 */
		return;

	pgm_init_shmem();

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
		result = standard_planner(parse, query_string, cursorOptions, boundParams);;

	if (!IsTransactionState() || !get_extension_oid(MODULENAME, true))
		/*
		 * Our extension doesn't exist in the database the backend is
		 * registered in, do nothing.
		 */
		return result;

	pgm_init_shmem();

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
	if (state == NULL)
		return;

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
on_prepare(PreparedStatement *ps)
{
	uint64				queryId = get_prepared_stmt_queryId(ps);
	MentorTblEntry	   *entry;
	LocaLPSEntry	   *lentry;
	bool				found;
	bool				found1;
	uint32				refcounter;

	if (queryId == UINT64CONST(0))
		return -1;

	entry = (MentorTblEntry *) dshash_find_or_insert(pgm_hash, &queryId, &found);

	if (found)
		entry->refcounter++;
	else
	{
		/* Initialise new entry */
		entry->refcounter = 1;
		entry->plan_cache_mode = get_plan_cache_mode(ps);
		entry->since = GetCurrentTimestamp();
		entry->ref_exec_time = -1.0;
	}
	refcounter = entry->refcounter;
	dshash_release_lock(pgm_hash, entry);

	/* Don't forget to insert it locally */
	lentry = (LocaLPSEntry *) hash_search(pgm_local_hash,
										  &queryId, HASH_ENTER, &found1);
	if (!found1)
		lentry->refcounter = 1;
	else
		lentry->refcounter++;

	/* If the entry doesn't exist in global entry it can't be in the local one */
	Assert(!(!found && found1));

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
 * Some sort of deallocation is coming.
 *
 * Find the record in global HTAB and decrease refcounter.
 * Remove the record from the local HTAB.
 */
static void
on_deallocate(uint64 queryId)
{
	MentorTblEntry	   *entry;
	LocaLPSEntry	   *le;
	bool				found;

	if (queryId != UINT64CONST(0))
	{
		le = (LocaLPSEntry *) hash_search(pgm_local_hash,
										  &queryId, HASH_FIND, &found);
		le->refcounter--;
		if (le->refcounter == 0)
			(void) hash_search(pgm_local_hash, &queryId, HASH_REMOVE, NULL);

		if (found)
		{
			entry = (MentorTblEntry *) dshash_find(pgm_hash, &queryId, true);
			if (entry != NULL)
			{
				entry->refcounter--;
				Assert(entry->refcounter < UINT32_MAX - 1);

				dshash_release_lock(pgm_hash, entry);
			}
			else
			{
				/* XXX: Is this possible? */
			}

		}
		else
		{
			/* XXX: Is this possible? */
		}
	}
	else
	{
		HASH_SEQ_STATUS hash_seq;

		/*
		 * Remove each prepared statement, registered in this backend.
		 */

		hash_seq_init(&hash_seq, pgm_local_hash);
		while ((le = hash_seq_search(&hash_seq)) != NULL)
		{
			Assert(le->queryId != UINT64CONST(0));

			entry = (MentorTblEntry *) dshash_find(pgm_hash, &le->queryId, true);
			if (entry != NULL)
			{
				entry->refcounter -= le->refcounter;
				Assert(entry->refcounter < UINT32_MAX - 1);

				dshash_release_lock(pgm_hash, entry);
			}
			else
			{
				/* XXX: Is this possible? */
			}

			/*
			 * Sometimes we may not find this entry in global HTAB having in the
			 * local one (reset). But still should delete it locally.
			 */
			(void) hash_search(pgm_local_hash, &le->queryId, HASH_REMOVE, NULL);
		}
	}
}

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

	pgm_init_shmem();

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
	/* Cache oid for further direct calls */
	psfuncoid = fmgr_internal_function(psfuncname);
	Assert(psfuncoid != InvalidOid);

	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pgm_post_parse_analyze;
	prev_planner_hook = planner_hook;
	planner_hook = pgm_planner;
	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = pgm_ProcessUtility_hook;

	recreate_local_htab();

	MarkGUCPrefixReserved(MODULENAME);
}
