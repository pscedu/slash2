# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		mount_slash
BIN=		mount_slash.sh
MAN+=		mount_slash.8
SRCS+=		bflush.c
SRCS+=		bmap_cli.c
SRCS+=		cfg_cli.c
SRCS+=		ctl_cli.c
SRCS+=		dircache.c
SRCS+=		fidc_cli.c
SRCS+=		main.c
SRCS+=		mount_slash_int.c
SRCS+=		pgcache.c
SRCS+=		rci.c
SRCS+=		rcm.c
SRCS+=		rpc_cli.c
SRCS+=		timer_cli.c
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
SRCS+=		${PFL_BASE}/eqpollthr.c
SRCS+=		${PFL_BASE}/fault.c
SRCS+=		${PFL_BASE}/hashtbl.c
SRCS+=		${PFL_BASE}/init.c
SRCS+=		${PFL_BASE}/iostats.c
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
SRCS+=		${PFL_BASE}/str.c
SRCS+=		${PFL_BASE}/stree.c
SRCS+=		${PFL_BASE}/subsys.c
SRCS+=		${PFL_BASE}/thread.c
SRCS+=		${PFL_BASE}/timerthr.c
SRCS+=		${PFL_BASE}/usklndthr.c
SRCS+=		${PFL_BASE}/vbitmap.c
SRCS+=		${PFL_BASE}/waitq.c
SRCS+=		${PFL_BASE}/workthr.c

SRCS+=		${SETRESUID_SRC}

DEFINES+=	-D_SLASH_CLIENT
#DEFINES+=	-DFHENT_EARLY_RELEASE

MODULES+=	lnet pthread gcrypt pscfs ctl rpc clock

include ${SLASHMK}
