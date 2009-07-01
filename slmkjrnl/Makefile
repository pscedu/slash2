# $Id: Makefile 5427 2009-03-02 21:40:57Z yanovich $

include Makefile.path

PROG=		slmkjrnl
SRCS+=		slmkjrnl.c
SRCS+=		${PFL_BASE}/psc_util/journal.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c
SRCS+=		${PFL_BASE}/psc_util/pthrutil.c

INCLUDES+=	-I${LNET_BASE}/include
DEFINES+=	-DHAVE_LIBPTHREAD_DEFINED
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
LDFLAGS+=       ${THREAD_LIBS} -lrt

include ${SLASHMK}
