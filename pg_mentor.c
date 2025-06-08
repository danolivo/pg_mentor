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
PG_FUNCTION_INFO_V1(reconsider_ps_modes);

static const char  *psfuncname = "pg_prepared_statement";
static Oid			psfuncoid = 0;
static int			nesting_level = 0;

#define pgm_enabled(level) \
	(!IsParallelWorker() && (level) == 0)

/*
 * List of intercepted hooks.
 *
 * Each hook should cehck existence of the extension. In case it doesn't exist
 * it should detach from shared structures, if existed.
 */
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

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

#define MENTOR_TBL_ENTRY_FIELDS_NUM	(13)
#define MENTOR_TBL_ENTRY_STAT_SIZE	(10)

typedef struct MentorTblEntry
{
	uint64		queryid; /* the key */
	uint32		refcounter; /* How much users use this statement? */
	int			plan_cache_mode;
	TimestampTz	since; /* The moment of addition to the table */

	double		ref_exec_time; /* execution time before the switch (or -1) */
	bool		fixed; /* May it be changed automatically? */

	/* Statistics */
	int64		nblocks[MENTOR_TBL_ENTRY_STAT_SIZE];
	double		times[MENTOR_TBL_ENTRY_STAT_SIZE];
	int			next_idx;
	double		avg_nblocks;
	double		ref_nblocks;
	double		avg_exec_time;
	double		plan_time;
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
	double	plan_time;
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
	dshash_seq_status	hash_seq;
	uint64				generation;
	MentorTblEntry	   *entry;
	List			   *pslst;

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

static bool
pg_mentor_set_plan_mode_int(MentorTblEntry *entry, int status,
							double ref_exec_time, double ref_nblocks, bool fixed)
{
	entry->plan_cache_mode = status;
	entry->fixed = fixed;

	if (entry->nblocks[0] < 0 && (ref_nblocks < 0. || ref_exec_time < 0.))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("reference data cannot be null for never executed query")));

	entry->ref_nblocks = (ref_nblocks > 0.) ?
											ref_nblocks : entry->avg_nblocks;
	entry->ref_exec_time = (ref_exec_time > 0.) ?
											ref_exec_time : entry->avg_exec_time;

	/* Tell other backends that they may update their statuses. */
	move_mentor_status();
	return true;
}

Datum
pg_mentor_set_plan_mode(PG_FUNCTION_ARGS)
{
	int64			queryId = PG_GETARG_INT64(0);
	int				status = PG_GETARG_INT32(1);
	double			ref_exec_time = PG_ARGISNULL(2) ? -1. : PG_GETARG_FLOAT8(2);
	double			ref_nblocks = PG_ARGISNULL(3) ? -1. : PG_GETARG_FLOAT8(3);
	bool			fixed = PG_GETARG_BOOL(4);
	bool			found;
	MentorTblEntry *entry;
	bool			result = false;

	pgm_init_shmem();

	entry = (MentorTblEntry *) dshash_find_or_insert(pgm_hash, &queryId, &found);
	result = pg_mentor_set_plan_mode_int(entry, status, ref_exec_time,
										 ref_nblocks, fixed);

	dshash_release_lock(pgm_hash, entry);
	PG_RETURN_BOOL(result);
}

/*
 * Return the ring buffer size.
 * It may contain only MENTOR_TBL_ENTRY_STAT_SIZE elements or entry->next_idx
 * elements in case it is not full yet.
 */
static int
ring_buffer_size(MentorTblEntry *entry)
{
	if (unlikely(entry->nblocks[entry->next_idx % MENTOR_TBL_ENTRY_STAT_SIZE] < 0))
		return entry->next_idx;
	else
		return MENTOR_TBL_ENTRY_STAT_SIZE;
}

static ArrayType *
form_vector_int64(int64 *vector, int nrows)
{
	Datum	   *elems;
	ArrayType  *a;
	int			lbs = 1;
	int			i;

	elems = palloc(sizeof(*elems) * nrows);
	for (i = 0; i < nrows; i++)
		elems[i] = Int64GetDatum(vector[i]);
	a = construct_md_array(elems, NULL, 1, &nrows, &lbs, INT8OID, 8, true, 'd');
	return a;
}

static ArrayType *
form_vector_dbl(double *vector, int nrows)
{
	Datum	   *elems;
	ArrayType  *a;
	int			lbs = 1;
	int			i;

	elems = palloc(sizeof(*elems) * nrows);
	for (i = 0; i < nrows; i++)
		elems[i] = Float8GetDatum(vector[i]);
	a = construct_md_array(elems, NULL, 1, &nrows, &lbs, FLOAT8OID, 8, true, 'd');
	return a;
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
		int		statnum;

		/* Do we need to skip this record? */
		if (status >= 0 && status != entry->plan_cache_mode)
			continue;

		values[0] = Int64GetDatumFast((int64) entry->queryid);
		values[1] = UInt64GetDatum(entry->refcounter);
		values[2] = Int32GetDatum(entry->plan_cache_mode);
		values[3] = TimestampTzGetDatum(entry->since);
		values[4] = BoolGetDatum(entry->fixed);

		statnum = ring_buffer_size(entry);
		values[5] = Int32GetDatum(statnum);
		if (statnum == 0)
		{
			nulls[6] = nulls[7] = nulls[8] = nulls[9] = true;
		}
		else
		{
			values[6] = PointerGetDatum(form_vector_int64(entry->nblocks, statnum));
			values[7] = PointerGetDatum(form_vector_dbl(entry->times, statnum));
			values[8] = Float8GetDatum(entry->avg_nblocks);
			values[9] = Float8GetDatum(entry->avg_exec_time);
		}

		if (entry->ref_nblocks > 0)
			values[10] = Float8GetDatum(entry->ref_nblocks);
		else
			nulls[10] = true;
		if (entry->ref_exec_time > 0.)
			values[11] = Float8GetDatum(entry->ref_exec_time);
		else
			nulls[11] = true;
		if (entry->plan_time >= 0.)
			values[12] = Float8GetDatum(entry->plan_time);
		else
			nulls[12] = true;

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	dshash_seq_term(&hash_seq);

	return (Datum) 0;
}

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

Datum
reconsider_ps_modes(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	dshash_seq_status	hash_seq;
	MentorTblEntry	   *entry;
	HeapTuple			tuple;
	int32				to_generic = 0;
	int32				to_custom = 0;
	int32				nvalues = 0;
	Datum				values[3] = {0};
	bool				nulls[3] = {0};
	double				stddev;

	pgm_init_shmem();

//	InitMaterializedSRF(fcinfo, 0);

	dshash_seq_init(&hash_seq, pgm_hash, false);
	while ((entry = dshash_seq_next(&hash_seq)) != NULL)
	{
		int	statnum = ring_buffer_size(entry);

		nvalues++;

		/* Do we need to skip this record? */
		if (entry->plan_cache_mode < 0)
			continue;

		if (entry->avg_nblocks <= 0. || statnum <= 1)
			continue;

		stddev = calculateStandardDeviation(statnum, entry->nblocks);

		/* Step 1: auto-mode => generic */
		if (entry->plan_cache_mode == 0 && !entry->fixed &&
			entry->ref_exec_time < 0. &&
			entry->avg_exec_time < entry->plan_time &&
			stddev / entry->avg_nblocks <= 0.3)
		{
			pg_mentor_set_plan_mode_int(entry, 1, -1, -1, false);
			to_generic++;
		}
		/* Step 2: */
		else if (entry->plan_cache_mode == 1 && !entry->fixed &&
			entry->ref_exec_time > 0. &&
			entry->avg_exec_time < entry->plan_time * 2.0 &&
			entry->avg_nblocks/entry->ref_nblocks > 1.0)
		{
			pg_mentor_set_plan_mode_int(entry, 2, -1, -1, false);
			to_custom++;
		}
		/* Step 3: auto-mode => custom */
		else if (entry->plan_cache_mode == 0 && !entry->fixed &&
			entry->ref_exec_time <= 0. &&
			entry->avg_exec_time > entry->plan_time * 1.0 &&
			stddev / entry->avg_nblocks > 0.5)
		{
			pg_mentor_set_plan_mode_int(entry, 2, -1, -1, false);
			to_custom++;
		}
		/* Step 4: 'custom' => 'generic' */
		else if (entry->plan_cache_mode == 2 && !entry->fixed &&
			entry->ref_exec_time > 0. &&
			(entry->avg_exec_time < entry->plan_time * 2.0 ||
			entry->ref_nblocks / entry->avg_nblocks < 2.0) &&
			stddev / entry->avg_nblocks <= 0.3)
		{
			pg_mentor_set_plan_mode_int(entry, 1, -1, -1, false);
			to_generic++;
		}
		else
		{
			/* Skip the record */
		}
	}
	dshash_seq_term(&hash_seq);

	values[0] = Int32GetDatum(to_generic);
	values[1] = Int32GetDatum(to_custom);
	values[2] = Int32GetDatum(nvalues - to_generic - to_custom);

	tuple = heap_form_tuple(rsinfo->expectedDesc, values, nulls);
	return HeapTupleGetDatum(tuple);
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
		int i;

		entry->plan_cache_mode = 0;
		entry->fixed = false;
		entry->ref_exec_time = -1;
		entry->since = 0;
		entry->next_idx = 0;
		entry->ref_exec_time = -1.0;
		entry->ref_nblocks = -1.;
		entry->avg_exec_time = 0.;
		entry->avg_nblocks = 0.;
		counter++;
		for (i = 0; i < MENTOR_TBL_ENTRY_STAT_SIZE; i++)
			entry->nblocks[i] = -1;
		for (i = 0; i < MENTOR_TBL_ENTRY_STAT_SIZE; i++)
			entry->times[i] = -1;
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

	if (pgm_enabled(nesting_level) && query_string
		&& parse->queryId != INT64CONST(0) && IsTransactionState() &&
		get_extension_oid(MODULENAME, true))
	{
		instr_time	start;
		instr_time	duration;
		bool			found;

		INSTR_TIME_SET_CURRENT(start);

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

		INSTR_TIME_SET_CURRENT(duration);
		INSTR_TIME_SUBTRACT(duration, start);

		pgm_init_shmem();
		check_state();

		/* Be gentle and track queries are known as prepared statements */
		(void) hash_search(pgm_local_hash, &result->queryId, HASH_FIND, &found);
		if (found)
		{
			MentorTblEntry *entry;

			entry = (MentorTblEntry *) dshash_find(pgm_hash, &result->queryId,
												   true);
			Assert(entry != NULL);
			entry->plan_time = INSTR_TIME_GET_MILLISEC(duration);
			dshash_release_lock(pgm_hash, entry);
		}
	}
	else
	{
		nesting_level++;
		PG_TRY();
		{
			if (prev_planner_hook)
				result = prev_planner_hook(parse, query_string, cursorOptions,
										   boundParams);
			else
				result = standard_planner(parse, query_string, cursorOptions,
										  boundParams);
		}
		PG_FINALLY();
		{
			nesting_level--;
		}
		PG_END_TRY();
	}

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
		int i;

		/* Initialise new entry */
		entry->refcounter = 1;
		entry->plan_cache_mode = get_plan_cache_mode(ps);
		entry->since = GetCurrentTimestamp();
		entry->ref_exec_time = -1.0;
		entry->next_idx = 0;
		entry->ref_nblocks = -1.;
		entry->avg_nblocks = 0.;
		entry->avg_exec_time = 0.;
		for (i = 0; i < MENTOR_TBL_ENTRY_STAT_SIZE; i++)
			entry->nblocks[i] = -1;
		for (i = 0; i < MENTOR_TBL_ENTRY_STAT_SIZE; i++)
			entry->times[i] = -1;
	}
	refcounter = entry->refcounter;
	dshash_release_lock(pgm_hash, entry);

	/* Don't forget to insert it locally */
	lentry = (LocaLPSEntry *) hash_search(pgm_local_hash,
										  &queryId, HASH_ENTER, &found1);
	if (!found1)
	{
		lentry->refcounter = 1;
		lentry->plan_time = -1.;
	}
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
on_execute(uint64 queryId, BufferUsage *bufusage, double exec_time)
{
	MentorTblEntry	   *entry;
	int64				nblocks;

	if (queryId == UINT64CONST(0))
		return;

	nblocks = bufusage->shared_blks_hit + bufusage->shared_blks_read +
				bufusage->local_blks_hit +bufusage->local_blks_read +
				bufusage->temp_blks_read;

	entry = (MentorTblEntry *) dshash_find(pgm_hash, &queryId, true);
	Assert(entry != NULL);
	Assert(ring_buffer_size(entry) <= MENTOR_TBL_ENTRY_STAT_SIZE);

	/*
	 * Calculate statistics. Be careful - in case of massive ring buffer
	 * computation on each execution may become costly.
	 */
	if (ring_buffer_size(entry) == MENTOR_TBL_ENTRY_STAT_SIZE)
	{
		entry->avg_nblocks +=
				(-entry->nblocks[entry->next_idx % MENTOR_TBL_ENTRY_STAT_SIZE] +
										nblocks) / MENTOR_TBL_ENTRY_STAT_SIZE;
		entry->avg_exec_time +=
				(-entry->times[entry->next_idx % MENTOR_TBL_ENTRY_STAT_SIZE] +
										exec_time) / MENTOR_TBL_ENTRY_STAT_SIZE;
	}
	else
	{
		entry->avg_nblocks = (entry->avg_nblocks * entry->next_idx + nblocks) /
														(entry->next_idx + 1);
		entry->avg_exec_time = (entry->avg_exec_time * entry->next_idx + exec_time) /
														(entry->next_idx + 1);
	}

	entry->nblocks[entry->next_idx % MENTOR_TBL_ENTRY_STAT_SIZE] = nblocks;
	entry->times[entry->next_idx % MENTOR_TBL_ENTRY_STAT_SIZE] = exec_time;
	entry->next_idx++;

	dshash_release_lock(pgm_hash, entry);
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
	Node	   *parsetree = pstmt->utilityStmt;
	uint64		queryId = UINT64CONST(0);
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

static void
pgm_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	uint64		queryId = queryDesc->plannedstmt->queryId;

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (pgm_enabled(nesting_level) && queryId != UINT64CONST(0) &&
		((eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0))
	{
		bool			found;

		/* Be gentle and track queries are known as prepared statements */
		(void) hash_search(pgm_local_hash, &queryId, HASH_FIND, &found);
		if (!found)
			return;

		if (queryDesc->totaltime == NULL)
		{
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
			queryDesc->totaltime =
					InstrAlloc(1, INSTRUMENT_BUFFERS | INSTRUMENT_TIMER, false);
			MemoryContextSwitchTo(oldcxt);
		}
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

	if (queryId != UINT64CONST(0) && queryDesc->totaltime &&
		pgm_enabled(nesting_level) &&
		((queryDesc->estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0))
	{
		bool	found;

		(void) hash_search(pgm_local_hash, &queryId, HASH_FIND, &found);
		if (found)
		{
			InstrEndLoop(queryDesc->totaltime);

			on_execute(queryId, &queryDesc->totaltime->bufusage,
					   queryDesc->totaltime->total * 1000.0);
		}
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
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
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgm_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pgm_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pgm_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgm_ExecutorEnd;

	recreate_local_htab();

	MarkGUCPrefixReserved(MODULENAME);
}
