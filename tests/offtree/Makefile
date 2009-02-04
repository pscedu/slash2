# $Id$

include ../Makefile.path

PROG=		offtree_test
SRCS+=		offtree_test.c
SRCS+=		${SLASH_BASE}/slashd/buffer.c
SRCS+=		${SLASH_BASE}/slashd/fidcache.c
SRCS+=		${SLASH_BASE}/slashd/offtree.c
SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hash.c
SRCS+=		${PFL_BASE}/psc_ds/vbitmap.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/iostats.c
SRCS+=		${PFL_BASE}/psc_util/journal.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/mkdirs.c
SRCS+=		${PFL_BASE}/psc_util/palloc.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/threadtable.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c
SRCS+=		${PFL_BASE}/psc_util/list.c

DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD -DHAVE_LIBPTHREAD_DEFINED
INCLUDES+=	-I${SLASH_BASE}/slashd
INCLUDES+=	-I${LNET_BASE}/include
LDFLAGS+=	${THREAD_LIBS} -lrt
CFLAGS+= -O2

include ${SLASHMK}
