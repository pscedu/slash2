# $Id$

include Makefile.path

PROG=		slashd
SRCS+=		buffer.c
SRCS+=		cfd.c
SRCS+=		control.c
SRCS+=		dircache.c
SRCS+=		fidlink.c
SRCS+=		lconf.l
SRCS+=		main.c
SRCS+=		metadir.c
SRCS+=		offtree.c
SRCS+=		rcm.c
SRCS+=		rmc.c
SRCS+=		rmi.c
SRCS+=		rmm.c
SRCS+=		rmmc.c
SRCS+=		rpc.c
SRCS+=		sb.c
SRCS+=		sljournal.c
SRCS+=		timer.c
SRCS+=		yconf.y
SRCS+=		${SLASH_BASE}/share/fid.c
SRCS+=		${SLASH_BASE}/share/rpc.c
SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hash.c
SRCS+=		${PFL_BASE}/psc_ds/stree.c
SRCS+=		${PFL_BASE}/psc_ds/vbitmap.c
SRCS+=		${PFL_BASE}/psc_rpc/rsx.c
SRCS+=		${PFL_BASE}/psc_util/acsvc.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/ctlsvr.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/iostats.c
SRCS+=		${PFL_BASE}/psc_util/journal.c
SRCS+=		${PFL_BASE}/psc_util/list.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/meter.c
SRCS+=		${PFL_BASE}/psc_util/mkdirs.c
SRCS+=		${PFL_BASE}/psc_util/strlcpy.c
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
LDFLAGS+=	${THREAD_LIBS} -lrt

include ${SLASHMK}
