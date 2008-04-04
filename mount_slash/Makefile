# $Id$

include Makefile.path

PROG=		mount_slash
SRCS=		mount_slash.c
SRCS+=		rpc.c
SRCS+=		rpcio.c
SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hash.c
SRCS+=		${PFL_BASE}/psc_rpc/connection.c
SRCS+=		${PFL_BASE}/psc_rpc/events.c
SRCS+=		${PFL_BASE}/psc_rpc/export.c
SRCS+=		${PFL_BASE}/psc_rpc/import.c
SRCS+=		${PFL_BASE}/psc_rpc/nb.c
SRCS+=		${PFL_BASE}/psc_rpc/niobuf.c
SRCS+=		${PFL_BASE}/psc_rpc/packgeneric.c
SRCS+=		${PFL_BASE}/psc_rpc/rpcclient.c
SRCS+=		${PFL_BASE}/psc_rpc/rsx.c
SRCS+=		${PFL_BASE}/psc_rpc/service.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/threadtable.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c

SRCS+=		${LNET_BASE}/socklnd/connection.c
SRCS+=		${LNET_BASE}/socklnd/pqtimer.c
SRCS+=		${LNET_BASE}/socklnd/procapi.c
SRCS+=		${LNET_BASE}/socklnd/proclib.c
SRCS+=		${LNET_BASE}/socklnd/select.c
SRCS+=		${LNET_BASE}/socklnd/table.c
SRCS+=		${LNET_BASE}/socklnd/tcplnd.c
SRCS+=		${LNET_BASE}/socklnd/sendrecv.c

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

CFLAGS+=	`pkg-config --cflags fuse`
LDFLAGS+=	`pkg-config --libs fuse` ${THREAD_LIBS}
INCLUDES+=	-I${LNET_BASE}/include
INCLUDES+=	-I${LNET_BASE}/socklnd
INCLUDES+=	-I${LNET_BASE}/libcfs
INCLUDES+=	-I${LNET_BASE}/lnet
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD -DHAVE_LIBPTHREAD_DEFINED

include ${SLASHMK}
