# contrib/pg_mentor/Makefile

MODULE_big	= pg_mentor
OBJS = \
	$(WIN32RES) \
	pg_mentor.o

REGRESS = automode

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_mentor
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

