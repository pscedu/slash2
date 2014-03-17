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

MODULES+=	lnet pthread rpc pfl

include ${SLASHMK}
