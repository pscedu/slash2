# $Id$

include ../Makefile.path

PROG=		replbit_test
SRCS+=		replbit_test.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${SLASH_BASE}/share/slerr.c

DEFINES+=	-DHAVE_LIBPTHREAD_DEFINED -DHAVE_GETHOSTBYNAME	\
		-DHAVE_GETHOSTBYNAME_DEFINED

include ${SLASHMK}
