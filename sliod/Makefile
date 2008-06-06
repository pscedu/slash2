# $Id$

include Makefile.path

PROG=		sliod
SRCS+=		control.c
SRCS+=		main.c
SRCS+=		ric.c
SRCS+=		rii.c
SRCS+=		rim.c
SRCS+=		rpc.c
SRCS+=		timer.c
SRCS+=		${SLASH_BASE}/share/fid.c
SRCS+=		${SLASH_BASE}/slashd/cfd.c
SRCS+=		${SLASH_BASE}/slashd/lconf.l
SRCS+=		${SLASH_BASE}/slashd/yconf.y

SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hash.c
SRCS+=		${PFL_BASE}/psc_ds/stree.c
SRCS+=		${PFL_BASE}/psc_util/ctlsvr.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/iostats.c
SRCS+=		${PFL_BASE}/psc_util/journal.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/mkdirs.c
SRCS+=		${PFL_BASE}/psc_util/palloc.c
SRCS+=		${PFL_BASE}/psc_util/strlcpy.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/threadtable.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c

SRCS+=		${PFL_BASE}/psc_rpc/rsx.c
SRCS+=		${PSCRPC_SRCS}
SRCS+=		${LNET_SOCKLND_SRCS}
SRCS+=		${LNET_LIB_SRCS}
SRCS+=		${LNET_CFS_SRCS}

INCLUDES+=	-I${LNET_BASE}/include
INCLUDES+=	-I${LNET_BASE}/lnet
INCLUDES+=	-I${LNET_BASE}/libcfs
INCLUDES+=	-I${LNET_BASE}/socklnd
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD -DHAVE_LIBPTHREAD_DEFINED
LDFLAGS+=	${THREAD_LIBS}

include ${SLASHMK}
