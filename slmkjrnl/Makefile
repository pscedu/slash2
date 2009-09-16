# $Id$

include Makefile.path

PROG=		slmkjrnl
SRCS+=		slmkjrnl.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/journal.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c
SRCS+=		${PFL_BASE}/psc_ds/dynarray.c

DEFINES+=	-DHAVE_LIBPTHREAD_DEFINED
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED

include ${SLASHMK}
