# $Id$

include Makefile.path

PROG=		slashd
MAN+=		slashd.8
SRCS+=		cfg_mds.c
SRCS+=		ctl_mds.c
SRCS+=		fidc_mds.c
SRCS+=		jflush.c
SRCS+=		main.c
SRCS+=		mds.c
SRCS+=		mdscoh.c
SRCS+=		mdsfssync.c
SRCS+=		mdsio_zfs.c
SRCS+=		mdslog.c
SRCS+=		metadir.c
SRCS+=		rcmc.c
SRCS+=		repl_mds.c
SRCS+=		rmc.c
SRCS+=		rmi.c
SRCS+=		rmm.c
SRCS+=		rpc_mds.c
SRCS+=		sitemon.c
SRCS+=		timer_mds.c
SRCS+=		${SLASH_BASE}/share/bmap.c
SRCS+=		${SLASH_BASE}/share/cfg_common.c
SRCS+=		${SLASH_BASE}/share/fdbuf.c
SRCS+=		${SLASH_BASE}/share/fid.c
SRCS+=		${SLASH_BASE}/share/fidc_common.c
SRCS+=		${SLASH_BASE}/share/lconf.l
SRCS+=		${SLASH_BASE}/share/mkfn.c
SRCS+=		${SLASH_BASE}/share/rpc_common.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${SLASH_BASE}/share/slutil.c
SRCS+=		${SLASH_BASE}/share/yconf.y

SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hash2.c
SRCS+=		${PFL_BASE}/psc_ds/stree.c
SRCS+=		${PFL_BASE}/psc_ds/vbitmap.c
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
SRCS+=		${PFL_BASE}/psc_util/multiwait.c
SRCS+=		${PFL_BASE}/psc_util/odtable.c
SRCS+=		${PFL_BASE}/psc_util/pool.c
SRCS+=		${PFL_BASE}/psc_util/pthrutil.c
SRCS+=		${PFL_BASE}/psc_util/random.c
SRCS+=		${PFL_BASE}/psc_util/rlimit.c
SRCS+=		${PFL_BASE}/psc_util/strlcat.c
SRCS+=		${PFL_BASE}/psc_util/strlcpy.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/timerthr.c
SRCS+=		${PFL_BASE}/psc_util/usklndthr.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c

SRCS+=		${PSCRPC_SRCS}
SRCS+=		${LNET_SOCKLND_SRCS}

DEFINES+=	-DZPOOL_PATH=\"$(realpath ${ZFS_BASE}/cmd/zpool)\"
LDFLAGS+=	-lz -laio -lgcrypt
MODULES+=	lnet pthread zfs

include ${SLASHMK}

zbuild:
	@(cd ${ZFS_BASE} && ${SCONS} ${ZFS_SCONSOPTS} -c build &&	\
	    ${SCONS} ${ZFS_SCONSOPTS})
	@(cd ${ZFS_BASE} &&						\
	    ${SCONS} slashlib=1 debug=4 ${ZFS_SCONSOPTS} -c build &&	\
	    ${SCONS} slashlib=1 debug=4 ${ZFS_SCONSOPTS})

build-prereq rezbuild:
	@(cd ${ZFS_BASE} && ${SCONS} slashlib=1 debug=4 ${ZFS_SCONSOPTS})
