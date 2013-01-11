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

SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hashtbl.c
SRCS+=		${PFL_BASE}/psc_ds/listcache.c
SRCS+=		${PFL_BASE}/psc_ds/stree.c
SRCS+=		${PFL_BASE}/psc_ds/vbitmap.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/base64.c
SRCS+=		${PFL_BASE}/psc_util/crc.c
SRCS+=		${PFL_BASE}/psc_util/eqpollthr.c
SRCS+=		${PFL_BASE}/psc_util/fault.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/iostats.c
SRCS+=		${PFL_BASE}/psc_util/list.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/meter.c
SRCS+=		${PFL_BASE}/psc_util/mkdirs.c
SRCS+=		${PFL_BASE}/psc_util/mlist.c
SRCS+=		${PFL_BASE}/psc_util/multiwait.c
SRCS+=		${PFL_BASE}/psc_util/netutil.c
SRCS+=		${PFL_BASE}/psc_util/pool.c
SRCS+=		${PFL_BASE}/psc_util/pthrutil.c
SRCS+=		${PFL_BASE}/psc_util/random.c
SRCS+=		${PFL_BASE}/psc_util/rlimit.c
SRCS+=		${PFL_BASE}/psc_util/str.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/timerthr.c
SRCS+=		${PFL_BASE}/psc_util/usklndthr.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c

SRCS+=		${SETRESUID_SRC}

DEFINES+=	-D_SLASH_CLIENT
#DEFINES+=	-DFHENT_EARLY_RELEASE

MODULES+=	lnet pthread gcrypt pscfs ctl rpc clock

include ${SLASHMK}
