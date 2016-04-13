# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slashd
BIN=		slashd.sh
MAN+=		slashd.8
MAN+=		slashd.sh.8
DEPLIST+=	${ZFS_BASE}:
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
SRCS+=		up_sched_res.c
SRCS+=		${OBJDIR}/rpc_names.c
SRCS+=		${SLASH_BASE}/share/authbuf_mgt.c
SRCS+=		${SLASH_BASE}/share/authbuf_sign.c
SRCS+=		${SLASH_BASE}/share/batchrpc.c
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
SRCS+=		${SLASH_BASE}/share/version.c
SRCS+=		${SLASH_BASE}/share/yconf.y

SRCS+=		${PFL_BASE}/journal.c
SRCS+=		${PFL_BASE}/fuse.c

DEFINES+=	-DZFS_BIN_PATH=\"$(realpath ${ZFS_BASE}/src/cmd/zfs)\"
DEFINES+=	-DZPOOL_PATH=\"$(realpath ${ZFS_BASE}/src/cmd/zpool)\"
DEFINES+=	-D_SLASH_MDS
MODULES+=	lnet pthread zfs z gcrypt fuse-hdrs aio pscfs ctl rpc sqlite pfl

include ${SLASHMK}

# XXX broken
$(call ADD_FILE_CFLAGS,${PFL_BASE}/journal.c,			\
		-DPSC_SUBSYS=SLMSS_JOURNAL -include subsys_mds.h)
