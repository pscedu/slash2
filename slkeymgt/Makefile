# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slkeymgt
MAN+=		slkeymgt.8
SRCS+=		slkeymgt.c
SRCS+=		${SLASH_BASE}/share/authbuf_mgt.c
SRCS+=		${SLASH_BASE}/share/mkfn.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${PFL_BASE}/alloc.c
SRCS+=		${PFL_BASE}/base64.c
SRCS+=		${PFL_BASE}/init.c
SRCS+=		${PFL_BASE}/log.c
SRCS+=		${PFL_BASE}/random.c

MODULES+=	lnet-hdrs gcrypt

include ${SLASHMK}
