# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slmkjrnl
MAN+=		slmkjrnl.8
SRCS+=		slmkjrnl.c
SRCS+=		${SLASH_BASE}/share/mkfn.c
SRCS+=		${SLASH_BASE}/share/slerr.c

MODULES+=	lnet-hdrs rt pthread aio pscfs pfl

include ${SLASHMK}
