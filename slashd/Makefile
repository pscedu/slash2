# $Id$

include Makefile.path

PROG=		slashd
SRCS+=		buffer.c
SRCS+=		cfd.c
SRCS+=		control.c
SRCS+=		fidc_mds.c
SRCS+=		jflush.c
SRCS+=		lconf.l
SRCS+=		main.c
SRCS+=		mds.c
SRCS+=		mdscoh.c
SRCS+=		mdsfssync.c
SRCS+=		mdslog.c
SRCS+=		mdsrepl.c
SRCS+=		mdsrpc.c
SRCS+=		metadir.c
SRCS+=		offtree.c
SRCS+=		rcm.c
SRCS+=		rmc.c
SRCS+=		rmi.c
SRCS+=		rmm.c
SRCS+=		rmmc.c
SRCS+=		sb.c
SRCS+=		sljournal.c
SRCS+=		timer.c
SRCS+=		yconf.y
SRCS+=		${SLASH_BASE}/share/fdbuf.c
SRCS+=		${SLASH_BASE}/share/fid.c
SRCS+=		${SLASH_BASE}/share/fidc_common.c
SRCS+=		${SLASH_BASE}/share/rpc.c
SRCS+=		${SLASH_BASE}/share/slashexport.c

SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hash.c
SRCS+=		${PFL_BASE}/psc_ds/hash2.c
SRCS+=		${PFL_BASE}/psc_ds/pool.c
SRCS+=		${PFL_BASE}/psc_ds/stree.c
SRCS+=		${PFL_BASE}/psc_ds/vbitmap.c
SRCS+=		${PFL_BASE}/psc_rpc/nb.c
SRCS+=		${PFL_BASE}/psc_rpc/rsx.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/base64.c
SRCS+=		${PFL_BASE}/psc_util/crc.c
SRCS+=		${PFL_BASE}/psc_util/ctlsvr.c
SRCS+=		${PFL_BASE}/psc_util/fault.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/iostats.c
SRCS+=		${PFL_BASE}/psc_util/journal.c
SRCS+=		${PFL_BASE}/psc_util/list.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/meter.c
SRCS+=		${PFL_BASE}/psc_util/mkdirs.c
SRCS+=		${PFL_BASE}/psc_util/pthrutil.c
SRCS+=		${PFL_BASE}/psc_util/random.c
SRCS+=		${PFL_BASE}/psc_util/rlimit.c
SRCS+=		${PFL_BASE}/psc_util/strlcpy.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/timerthr.c
SRCS+=		${PFL_BASE}/psc_util/usklndthr.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c
SRCS+=		${PFL_BASE}/psc_util/odtable.c

SRCS+=		${PSCRPC_SRCS}
SRCS+=		${LNET_SOCKLND_SRCS}
SRCS+=		${LNET_CFS_SRCS}
SRCS+=		${LNET_LIB_SRCS}

INCLUDES+=	-I${LNET_BASE}/include
INCLUDES+=	-I${LNET_BASE}
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD -DHAVE_LIBPTHREAD_DEFINED
LDFLAGS+=	${ZFS_LIBS} ${THREAD_LIBS} -ldl -lz -laio -lrt
LDFLAGS+=	-lgcrypt

include ${SLASHMK}
