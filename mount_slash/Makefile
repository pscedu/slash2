# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path
include ${SLASH_BASE}/mk/pre.mk

SHLIB=		slash2client.so
INST_LIBDIR=	${WOK_MOD_DIR}
BIN=		mount_slash.sh
MAN+=		slash2client.so.5
MAN+=		mount_slash.sh.8

SRCS+=		bflush.c
SRCS+=		bmap_cli.c
SRCS+=		cfg_cli.c
SRCS+=		ctl_cli.c
SRCS+=		dircache.c
SRCS+=		fidc_cli.c
SRCS+=		io.c
SRCS+=		main.c
SRCS+=		pgcache.c
SRCS+=		rci.c
SRCS+=		rcm.c
SRCS+=		rpc_cli.c
SRCS+=		usermap.c
SRCS+=		${OBJDIR}/rpc_names.c
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
SRCS+=		${SLASH_BASE}/share/version.c
SRCS+=		${SLASH_BASE}/share/yconf.y

MODULES+=	pthread gcrypt pscfs-hdrs rpc ctl pfl-hdrs
DEPLIST+=	${PFL_BASE}:libpfl.a

DEFINES+=	-D_SLASH_CLIENT -DSL_FAULT_PREFIX='"slash2/"'

include ${SLASHMK}
