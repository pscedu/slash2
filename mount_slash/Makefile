# $Id$

include Makefile.path

PROG=		mount_slash
SRCS+=		bmap_flush.c
SRCS+=		cli_ctl.c
SRCS+=		cli_slcfg.c
SRCS+=		cli_timer.c
SRCS+=		fidc_client.c
SRCS+=		fuse_listener.c
SRCS+=		main.c
SRCS+=		mount_slash_int.c
SRCS+=		mseqpollthr.c
SRCS+=		rcm.c
SRCS+=		rpc.c
SRCS+=		${SLASH_BASE}/share/bmap.c
SRCS+=		${SLASH_BASE}/share/buffer.c
SRCS+=		${SLASH_BASE}/share/fid.c
SRCS+=		${SLASH_BASE}/share/fidc_common.c
SRCS+=		${SLASH_BASE}/share/lconf.l
SRCS+=		${SLASH_BASE}/share/rpc_common.c
SRCS+=		${SLASH_BASE}/share/str.c
SRCS+=		${SLASH_BASE}/share/yconf.y
SRCS+=		${SLASH_BASE}/slashd/offtree.c

SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hash.c
SRCS+=		${PFL_BASE}/psc_ds/hash2.c
SRCS+=		${PFL_BASE}/psc_ds/pool.c
SRCS+=		${PFL_BASE}/psc_ds/stree.c
SRCS+=		${PFL_BASE}/psc_ds/vbitmap.c
SRCS+=		${PFL_BASE}/psc_rpc/nb.c
SRCS+=		${PFL_BASE}/psc_rpc/rsx.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/crc.c
SRCS+=		${PFL_BASE}/psc_util/ctlsvr.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/iostats.c
SRCS+=		${PFL_BASE}/psc_util/list.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/meter.c
SRCS+=		${PFL_BASE}/psc_util/mkdirs.c
SRCS+=		${PFL_BASE}/psc_util/pthrutil.c
SRCS+=		${PFL_BASE}/psc_util/rlimit.c
SRCS+=		${PFL_BASE}/psc_util/strlcpy.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/timerthr.c
SRCS+=		${PFL_BASE}/psc_util/usklndthr.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c

SRCS+=		${PSCRPC_SRCS}
SRCS+=		${LNET_CFS_SRCS}
SRCS+=		${LNET_LIB_SRCS}
SRCS+=		${LNET_SOCKLND_SRCS}

CFLAGS+=	${FUSE_CFLAGS}
DEFINES+=	${FUSE_DEFINES}
INCLUDES+=	${FUSE_INCLUDES}
INCLUDES+=	-I${LNET_BASE}/include
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD -DHAVE_LIBPTHREAD_DEFINED
LDFLAGS+=	${THREAD_LIBS} ${FUSE_LIBS}

include ${SLASHMK}
