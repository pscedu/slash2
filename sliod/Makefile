# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		sliod
BIN=		sliod.sh
MAN+=		sliod.8
MAN+=		sliod.sh.8
SRCS+=		bmap_iod.c
SRCS+=		cfg_iod.c
SRCS+=		ctl_iod.c
SRCS+=		fidc_iod.c
SRCS+=		main_iod.c
SRCS+=		repl_iod.c
SRCS+=		ric.c
SRCS+=		rii.c
SRCS+=		rim.c
SRCS+=		rmi.c
SRCS+=		rpc_iod.c
SRCS+=		slab.c
SRCS+=		slvr.c
SRCS+=		slvr_worker.c
SRCS+=		${OBJDIR}/rpc_names.c
SRCS+=		${SLASH_BASE}/share/adler32.c
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

MODULES+=	pthread lnet gcrypt rpc ctl clock futimens pfl

include ${SLASHMK}
