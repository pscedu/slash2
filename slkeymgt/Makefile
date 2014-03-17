# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slkeymgt
MAN+=		slkeymgt.8
SRCS+=		slkeymgt.c
SRCS+=		${SLASH_BASE}/share/authbuf_mgt.c
SRCS+=		${SLASH_BASE}/share/mkfn.c
SRCS+=		${SLASH_BASE}/share/slerr.c

MODULES+=	lnet-hdrs gcrypt pfl

include ${SLASHMK}
