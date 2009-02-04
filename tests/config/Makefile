# $Id$

include ../Makefile.path

PROG=		slconfig_test
SRCS+=		slconfig_test.c
SRCS+=		${SLASH_BASE}/slashd/lconf.l
SRCS+=		${SLASH_BASE}/slashd/yconf.y
SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hash.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/iostats.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c

SRCS+=		${LNET_CFS_SRCS}
SRCS+=		${LNET_LIB_SRCS}
SRCS+=		${LNET_SOCKLND_SRCS}
SRCS+=		${PSCRPC_SRCS}

INCLUDES+=	-I${LNET_BASE}/include
INCLUDES+=	-I${LNET_BASE}/libcfs
INCLUDES+=	-I${LNET_BASE}/lnet
INCLUDES+=	-I${LNET_BASE}/socklnd

LDFLAGS+=	${LIBL} ${LNET_LIBS} ${THREAD_LIBS}
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD_DEFINED -DHAVE_LIBPTHREAD

include ${SLASHMK}
