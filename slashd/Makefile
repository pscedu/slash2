# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slashd
BIN=		slashd.sh
MAN+=		slashd.8
SRCS+=		bmap_mds.c
SRCS+=		cfg_mds.c
SRCS+=		coh.c
SRCS+=		ctl_mds.c
SRCS+=		fidc_mds.c
SRCS+=		ino_compat.c
SRCS+=		inode.c
SRCS+=		jreplay.c
SRCS+=		main_mds.c
SRCS+=		mds.c
SRCS+=		mds_bmap_timeo.c
SRCS+=		mdsio_zfs.c
SRCS+=		mdslog.c
SRCS+=		odtable_mds.c
SRCS+=		rcmc.c
SRCS+=		repl_mds.c
SRCS+=		rmc.c
SRCS+=		rmi.c
SRCS+=		rmm.c
SRCS+=		rpc_mds.c
SRCS+=		timer_mds.c
SRCS+=		up_sched_res.c
SRCS+=		${SLASH_BASE}/share/authbuf_mgt.c
SRCS+=		${SLASH_BASE}/share/authbuf_sign.c
SRCS+=		${SLASH_BASE}/share/bmap.c
SRCS+=		${SLASH_BASE}/share/cfg_common.c
SRCS+=		${SLASH_BASE}/share/ctlsvr_common.c
SRCS+=		${SLASH_BASE}/share/fidc_common.c
SRCS+=		${SLASH_BASE}/share/lconf.l
SRCS+=		${SLASH_BASE}/share/mkfn.c
SRCS+=		${SLASH_BASE}/share/priv.c
SRCS+=		${SLASH_BASE}/share/rpc_common.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${SLASH_BASE}/share/slutil.c
SRCS+=		${SLASH_BASE}/share/yconf.y

SRCS+=		${PFL_BASE}/alloc.c
SRCS+=		${PFL_BASE}/base64.c
SRCS+=		${PFL_BASE}/crc.c
SRCS+=		${PFL_BASE}/dynarray.c
SRCS+=		${PFL_BASE}/fault.c
SRCS+=		${PFL_BASE}/hashtbl.c
SRCS+=		${PFL_BASE}/init.c
SRCS+=		${PFL_BASE}/iostats.c
SRCS+=		${PFL_BASE}/journal.c
SRCS+=		${PFL_BASE}/list.c
SRCS+=		${PFL_BASE}/listcache.c
SRCS+=		${PFL_BASE}/log.c
SRCS+=		${PFL_BASE}/meter.c
SRCS+=		${PFL_BASE}/mkdirs.c
SRCS+=		${PFL_BASE}/mlist.c
SRCS+=		${PFL_BASE}/multiwait.c
SRCS+=		${PFL_BASE}/netutil.c
SRCS+=		${PFL_BASE}/pool.c
SRCS+=		${PFL_BASE}/pthrutil.c
SRCS+=		${PFL_BASE}/random.c
SRCS+=		${PFL_BASE}/rlimit.c
SRCS+=		${PFL_BASE}/stree.c
SRCS+=		${PFL_BASE}/subsys.c
SRCS+=		${PFL_BASE}/thread.c
SRCS+=		${PFL_BASE}/timerthr.c
SRCS+=		${PFL_BASE}/usklndthr.c
SRCS+=		${PFL_BASE}/vbitmap.c
SRCS+=		${PFL_BASE}/waitq.c
SRCS+=		${PFL_BASE}/workthr.c

SRCS+=		${SETRESUID_SRC}

DEFINES+=	-DZFS_BIN_PATH=\"$(realpath ${ZFS_BASE}/src/cmd/zfs)\"
DEFINES+=	-DZPOOL_PATH=\"$(realpath ${ZFS_BASE}/src/cmd/zpool)\"
DEFINES+=	-D_SLASH_MDS
MODULES+=	lnet pthread zfs z gcrypt fuse-hdrs aio pscfs ctl rpc sqlite

include ${SLASHMK}

$(call ADD_FILE_CFLAGS,${PFL_BASE}/journal.c,			\
		-DPSC_SUBSYS=SLMSS_JOURNAL -include subsys_mds.h)
