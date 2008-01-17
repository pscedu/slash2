# $Id$

include Makefile.path

PROG=		slashd
SRCS+=		cfd.c
SRCS+=		dircache.c
SRCS+=		fidlink.c
SRCS+=		journal.c
SRCS+=		lconf.l
SRCS+=		main.c
SRCS+=		mds.c
SRCS+=		metadir.c
SRCS+=		rpc.c
SRCS+=		yconf.y
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_ds/hash.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/threadtable.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c
SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_rpc/connection.c
SRCS+=		${PFL_BASE}/psc_util/journal.c
SRCS+=		${PFL_BASE}/psc_util/palloc.c
SRCS+=		${PFL_BASE}/psc_rpc/events.c
SRCS+=		${PFL_BASE}/psc_rpc/export.c
SRCS+=		${PFL_BASE}/psc_rpc/import.c
SRCS+=		${PFL_BASE}/psc_rpc/nb.c
SRCS+=		${PFL_BASE}/psc_rpc/niobuf.c
SRCS+=		${PFL_BASE}/psc_rpc/packgeneric.c
SRCS+=		${PFL_BASE}/psc_rpc/rpcclient.c
SRCS+=		${PFL_BASE}/psc_rpc/service.c
INCLUDES+=	-I${LNET_BASE}/include
LDFLAGS+=	${THREAD_LIBS} ${LNET_LIBS}
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD -DHAVE_LIBPTHREAD_DEFINED

include ${SLASHMK}
