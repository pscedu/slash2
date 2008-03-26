# $Id$

include Makefile.path

PROG=		slashd
SRCS+=		backend.c
SRCS+=		cfd.c
SRCS+=		control.c
SRCS+=		dircache.c
SRCS+=		fidlink.c
SRCS+=		journal.c
SRCS+=		lconf.l
SRCS+=		main.c
SRCS+=		mds.c
SRCS+=		metadir.c
SRCS+=		rpc.c
SRCS+=		sb.c
SRCS+=		yconf.y
SRCS+=		${SLASH_BASE}/share/fid.c
SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hash.c
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

SRCS+=		${PFL_BASE}/psc_rpc/connection.c
SRCS+=		${PFL_BASE}/psc_rpc/events.c
SRCS+=		${PFL_BASE}/psc_rpc/export.c
SRCS+=		${PFL_BASE}/psc_rpc/import.c
SRCS+=		${PFL_BASE}/psc_rpc/nb.c
SRCS+=		${PFL_BASE}/psc_rpc/niobuf.c
SRCS+=		${PFL_BASE}/psc_rpc/packgeneric.c
SRCS+=		${PFL_BASE}/psc_rpc/rpcclient.c
SRCS+=		${PFL_BASE}/psc_rpc/service.c

SRCS+=		${LNET_BASE}/libcfs/debug.c
SRCS+=		${LNET_BASE}/libcfs/nidstrings.c
SRCS+=		${LNET_BASE}/libcfs/user-lock.c
SRCS+=		${LNET_BASE}/libcfs/user-prim.c

SRCS+=		${LNET_BASE}/lnet/acceptor.c
SRCS+=		${LNET_BASE}/lnet/api-errno.c
SRCS+=		${LNET_BASE}/lnet/api-ni.c
SRCS+=		${LNET_BASE}/lnet/config.c
SRCS+=		${LNET_BASE}/lnet/lib-eq.c
SRCS+=		${LNET_BASE}/lnet/lib-md.c
SRCS+=		${LNET_BASE}/lnet/lib-me.c
SRCS+=		${LNET_BASE}/lnet/lib-move.c
SRCS+=		${LNET_BASE}/lnet/lib-msg.c
SRCS+=		${LNET_BASE}/lnet/lo.c
SRCS+=		${LNET_BASE}/lnet/peer.c
SRCS+=		${LNET_BASE}/lnet/router.c
SRCS+=		${LNET_BASE}/lnet/router_proc.c

SRCS+=		${LNET_BASE}/socklnd/connection.c
SRCS+=		${LNET_BASE}/socklnd/pqtimer.c
SRCS+=		${LNET_BASE}/socklnd/procapi.c
SRCS+=		${LNET_BASE}/socklnd/proclib.c
SRCS+=		${LNET_BASE}/socklnd/select.c
SRCS+=		${LNET_BASE}/socklnd/table.c
SRCS+=		${LNET_BASE}/socklnd/tcplnd.c
SRCS+=		${LNET_BASE}/socklnd/sendrecv.c

INCLUDES+=	-I${LNET_BASE}/include
INCLUDES+=	-I${LNET_BASE}/socklnd
INCLUDES+=	-I${LNET_BASE}/libcfs
INCLUDES+=	-I${LNET_BASE}/lnet
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD -DHAVE_LIBPTHREAD_DEFINED
LDFLAGS+=	${THREAD_LIBS}

include ${SLASHMK}
