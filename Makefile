# contrib/pg_mentor/Makefile

MODULE_big	= pg_mentor
OBJS = \
	$(WIN32RES) \
	pg_mentor.o

EXTENSION = pg_mentor
DATA = pg_mentor--0.1.sql
PGFILEDESC = "pg_mentor - manage query parameters"

REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/pg_mentor/pg_mentor.conf
REGRESS = pg_mentor

EXTRA_INSTALL = contrib/pg_stat_statements

ISOLATION = pg_mentor_isolation
ISOLATION_OPTS = --temp-config $(top_srcdir)/contrib/pg_mentor/pg_mentor.conf \
				--load-extension=pg_stat_statements \
				--load-extension=pg_mentor

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