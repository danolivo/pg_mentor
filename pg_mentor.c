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
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "storage/lwlock.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"

#include "pg_mentor.h"

PG_MODULE_MAGIC_EXT(
					.name = MODULENAME,
					.version = PG_VERSION
);

static const char  *psfuncname = "pg_prepared_statement";
static Oid			psfuncoid = 0;

/*
 * List of intercepted hooks.
 *
 * Each hook should cehck existence of the extension. In case it doesn't exist
 * it should detach from shared structures, if existed.
 */
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

uint64
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

void
_PG_init(void)
{
	EnableQueryId();

	/* Cache oid for further direct calls */
	psfuncoid = fmgr_internal_function(psfuncname);
	Assert(psfuncoid != InvalidOid);

	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = pgm_ProcessUtility_hook;

	automode_init();

	MarkGUCPrefixReserved(MODULENAME);
}
