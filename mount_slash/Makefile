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
SRCS+=		${PFL_BASE}/fuse.c

DEFINES+=	-D_SLASH_CLIENT
#DEFINES+=	-DFHENT_EARLY_RELEASE

MODULES+=	lnet pthread gcrypt pscfs ctl rpc clock pfl

include ${SLASHMK}
