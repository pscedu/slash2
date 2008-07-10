# $Id$

include Makefile.path

PROG=		mount_slash
SRCS+=		control.c
SRCS+=		main.c
SRCS+=		mount_slash_int.c
SRCS+=		rpc.c
SRCS+=		${SLASH_BASE}/share/rpc.c
SRCS+=		${SLASH_BASE}/slashd/fidcache.c
SRCS+=		${SLASH_BASE}/slashd/offtree.c
SRCS+=		${SLASH_BASE}/slashd/buffer.c
SRCS+=          ${SLASH_BASE}/slashd/lconf.l
SRCS+=          ${SLASH_BASE}/slashd/yconf.y
SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hash.c
SRCS+=		${PFL_BASE}/psc_ds/stree.c
SRCS+=		${PFL_BASE}/psc_ds/vbitmap.c
SRCS+=		${PFL_BASE}/psc_mount/fh.c
SRCS+=		${PFL_BASE}/psc_rpc/rsx.c
SRCS+=		${PFL_BASE}/psc_util/ctlsvr.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/iostats.c
SRCS+=		${PFL_BASE}/psc_util/list.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/meter.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/threadtable.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c
SRCS+=		${PSCRPC_SRCS}
SRCS+=		${LNET_SOCKLND_SRCS}
SRCS+=		${LNET_CFS_SRCS}
SRCS+=		${LNET_LIB_SRCS}

CFLAGS+=	`pkg-config --cflags fuse`
LDFLAGS+=	`pkg-config --libs fuse` ${THREAD_LIBS}
INCLUDES+=	-I${LNET_BASE}/include
INCLUDES+=	-I${LNET_BASE}/socklnd
INCLUDES+=	-I${LNET_BASE}/libcfs
INCLUDES+=	-I${LNET_BASE}/lnet
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD -DHAVE_LIBPTHREAD_DEFINED

include ${SLASHMK}
