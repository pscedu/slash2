# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slmkfs
MAN+=		slmkfs.8
SRCS+=		slmkfs.c
SRCS+=		wipedir.c
SRCS+=		${SLASH_BASE}/share/cfg_common.c
SRCS+=		${SLASH_BASE}/share/lconf.l
SRCS+=		${SLASH_BASE}/share/mkfn.c
SRCS+=		${SLASH_BASE}/share/priv.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${SLASH_BASE}/share/yconf.y
SRCS+=		${PFL_BASE}/alloc.c
SRCS+=		${PFL_BASE}/dynarray.c
SRCS+=		${PFL_BASE}/hashtbl.c
SRCS+=		${PFL_BASE}/hostname.c
SRCS+=		${PFL_BASE}/init.c
SRCS+=		${PFL_BASE}/log.c
SRCS+=		${PFL_BASE}/netutil.c
SRCS+=		${PFL_BASE}/pthrutil.c
SRCS+=		${PFL_BASE}/random.c
SRCS+=		${PFL_BASE}/thread.c
SRCS+=		${PFL_BASE}/usklndthr.c
SRCS+=		${PFL_BASE}/waitq.c
SRCS+=		${SETRESUID_SRC}

MODULES+=	lnet pthread rpc

include ${SLASHMK}
