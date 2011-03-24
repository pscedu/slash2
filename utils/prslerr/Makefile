# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		prslerr
SRCS+=		prslerr.c
SRCS+=		${SLASH_BASE}/share/slerr.c

MODULES+=	pfl

include ${SLASHMK}

prslerr.c: ${SLASH_BASE}/include/slerr.h
	${ECHORUN} perl regen.pl prslerr.c ${SLASH_BASE}/include/slerr.h
