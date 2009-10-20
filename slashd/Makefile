# $Id$

include Makefile.path

PROG=		slashd
SRCS+=		fidc_mds.c
SRCS+=		jflush.c
SRCS+=		main.c
SRCS+=		mds.c
SRCS+=		mds_ctl.c
SRCS+=		mds_slcfg.c
SRCS+=		mds_timer.c
SRCS+=		mdscoh.c
SRCS+=		mdsfssync.c
SRCS+=		mdsio_zfs.c
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
SRCS+=		sljournal.c
SRCS+=		${SLASH_BASE}/share/bmap.c
SRCS+=		${SLASH_BASE}/share/buffer.c
SRCS+=		${SLASH_BASE}/share/cfd.c
SRCS+=		${SLASH_BASE}/share/fdbuf.c
SRCS+=		${SLASH_BASE}/share/fid.c
SRCS+=		${SLASH_BASE}/share/fidc_common.c
SRCS+=		${SLASH_BASE}/share/lconf.l
SRCS+=		${SLASH_BASE}/share/rpc_common.c
SRCS+=		${SLASH_BASE}/share/slashexport.c
SRCS+=		${SLASH_BASE}/share/str.c
SRCS+=		${SLASH_BASE}/share/yconf.y

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
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/iostats.c
SRCS+=		${PFL_BASE}/psc_util/journal.c
SRCS+=		${PFL_BASE}/psc_util/list.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/meter.c
SRCS+=		${PFL_BASE}/psc_util/mkdirs.c
SRCS+=		${PFL_BASE}/psc_util/odtable.c
SRCS+=		${PFL_BASE}/psc_util/pthrutil.c
SRCS+=		${PFL_BASE}/psc_util/random.c
SRCS+=		${PFL_BASE}/psc_util/rlimit.c
SRCS+=		${PFL_BASE}/psc_util/strlcpy.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/timerthr.c
SRCS+=		${PFL_BASE}/psc_util/usklndthr.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c
SRCS+=		${PFL_BASE}/psc_util/wndmap.c

#SRCS+=		${ZFS_BASE}/cmd/zpool/ptrace.c
#SRCS+=		${ZFS_BASE}/cmd/zpool/zpool_iter.c
#SRCS+=		${ZFS_BASE}/cmd/zpool/zpool_util.c
#SRCS+=		${ZFS_BASE}/cmd/zpool/zpool_vdev.c

SRCS+=		${PSCRPC_SRCS}
SRCS+=		${LNET_SOCKLND_SRCS}
SRCS+=		${LNET_CFS_SRCS}
SRCS+=		${LNET_LIB_SRCS}

INCLUDES+=	-I${LNET_BASE}/include
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD -DHAVE_LIBPTHREAD_DEFINED
DEFINES+=	-DZPOOL_PATH=\"${ZFS_BASE}/cmd/zpool\"
LDFLAGS+=	${ZFS_LIBS} ${ZPOOL_LIBS}
LDFLAGS+=	${THREAD_LIBS} -ldl -lz -laio -lrt -lgcrypt

# DEFINES+=	-DTEXT_DOMAIN=\"zfs-fuse\"
# ZFS_INCLUDES=	-I${ZFS_BASE}/lib/libavl/include				\
#		-I${ZFS_BASE}/lib/libnvpair/include				\
#		-I${ZFS_BASE}/lib/libsolcompat/include				\
#		-I${ZFS_BASE}/lib/libuutil/include				\
#		-I${ZFS_BASE}/lib/libzfs/include				\
#		-I${ZFS_BASE}/lib/libzfscommon/include
#
# zfs_zfs_fuse_0_5_0_slash_src_cmd_zpool_zpool_iter_c_CFLAGS+=	${ZFS_INCLUDES}
# zfs_zfs_fuse_0_5_0_slash_src_cmd_zpool_zpool_util_c_CFLAGS+=	${ZFS_INCLUDES}
# zfs_zfs_fuse_0_5_0_slash_src_cmd_zpool_zpool_vdev_c_CFLAGS+=	${ZFS_INCLUDES}
# zfs_zfs_fuse_0_5_0_slash_src_zfs_fuse_ptrace_c_CFLAGS+=		${ZFS_INCLUDES} -std=c99

include ${SLASHMK}
