# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slashd
MAN+=		slashd.8
SRCS+=		bmap_mds.c
SRCS+=		cfg_mds.c
SRCS+=		ctl_mds.c
SRCS+=		fidc_mds.c
SRCS+=		ino_compat.c
SRCS+=		inode.c
SRCS+=		main_mds.c
SRCS+=		mds.c
SRCS+=		mds_bmap_timeo.c
SRCS+=		mdscoh.c
SRCS+=		mdsio_zfs.c
SRCS+=		mdslog.c
SRCS+=		metadir.c
SRCS+=		odtable_mds.c
SRCS+=		rcmc.c
SRCS+=		repl_mds.c
SRCS+=		replay.c
SRCS+=		rmc.c
SRCS+=		rmi.c
SRCS+=		rmm.c
SRCS+=		rpc_mds.c
SRCS+=		timer_mds.c
SRCS+=		up_sched_res.c
SRCS+=		worker.c
SRCS+=		${SLASH_BASE}/share/authbuf_mgt.c
SRCS+=		${SLASH_BASE}/share/authbuf_sign.c
SRCS+=		${SLASH_BASE}/share/bmap.c
SRCS+=		${SLASH_BASE}/share/cfg_common.c
SRCS+=		${SLASH_BASE}/share/ctlsvr_common.c
SRCS+=		${SLASH_BASE}/share/fidc_common.c
SRCS+=		${SLASH_BASE}/share/lconf.l
SRCS+=		${SLASH_BASE}/share/mkfn.c
SRCS+=		${SLASH_BASE}/share/nbrqthr.c
SRCS+=		${SLASH_BASE}/share/priv.c
SRCS+=		${SLASH_BASE}/share/rpc_common.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${SLASH_BASE}/share/slutil.c
SRCS+=		${SLASH_BASE}/share/yconf.y

SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hashtbl.c
SRCS+=		${PFL_BASE}/psc_ds/listcache.c
SRCS+=		${PFL_BASE}/psc_ds/stree.c
SRCS+=		${PFL_BASE}/psc_ds/vbitmap.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/base64.c
SRCS+=		${PFL_BASE}/psc_util/crc.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/iostats.c
SRCS+=		${PFL_BASE}/psc_util/journal.c
SRCS+=		${PFL_BASE}/psc_util/list.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/meter.c
SRCS+=		${PFL_BASE}/psc_util/mkdirs.c
SRCS+=		${PFL_BASE}/psc_util/multiwait.c
SRCS+=		${PFL_BASE}/psc_util/netutil.c
SRCS+=		${PFL_BASE}/psc_util/pool.c
SRCS+=		${PFL_BASE}/psc_util/pthrutil.c
SRCS+=		${PFL_BASE}/psc_util/random.c
SRCS+=		${PFL_BASE}/psc_util/rlimit.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/timerthr.c
SRCS+=		${PFL_BASE}/psc_util/usklndthr.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c

SRCS+=		${SETRESUID_SRC}

DEFINES+=	-DZPOOL_PATH=\"$(realpath ${ZFS_BASE}/src/cmd/zpool)\"
DEFINES+=	-D_SLASH_MDS
MODULES+=	lnet pthread zfs z gcrypt fuse-hdrs aio pscfs ctl rpc

include ${SLASHMK}

$(call ADD_FILE_CFLAGS,${PFL_BASE}/psc_util/journal.c,			\
		-DPSC_SUBSYS=SLMSS_JOURNAL -include subsys_mds.h)
