#ifndef _PG_MENTOR_H_
#define _PG_MENTOR_H_

#include "postgres.h"

#include "access/parallel.h"
#include "commands/prepare.h"


#define MODULENAME	"pg_mentor"

#define pgm_enabled(level) \
	(!IsParallelWorker() && (level) == 0)

extern void automode_init(void);
extern void automode_on_prepare(PreparedStatement  *ps);
extern uint64 get_prepared_stmt_queryId(PreparedStatement  *ps);
extern void automode_on_deallocate(uint64 queryId, void *plansource_ptr);

#endif /* _PG_MENTOR_H_ */
