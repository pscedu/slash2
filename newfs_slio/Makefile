# $Id$

include Makefile.path

PROG=		newfs_slio
SRCS+=		newfs_slio.c
SRCS+=		${SLASH_BASE}/slashd/yconf.y
SRCS+=		${SLASH_BASE}/slashd/lconf.l

SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hash.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/iostats.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/threadtable.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c

SRCS+=		${PSCRPC_SRCS}
SRCS+=		${LNET_SOCKLND_SRCS}
SRCS+=		${LNET_CFS_SRCS}
SRCS+=		${LNET_LIB_SRCS}

INCLUDES+=	-I${LNET_BASE}/include
INCLUDES+=	-I${LNET_BASE}/socklnd
INCLUDES+=	-I${LNET_BASE}/libcfs
INCLUDES+=	-I${LNET_BASE}/lnet
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD -DHAVE_LIBPTHREAD_DEFINED
LDFLAGS+=	${THREAD_LIBS}

include ${SLASHMK}
