# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		prslerr
SRCS+=		prslerr.c
SRCS+=		${SLASH_BASE}/share/slerr.c

MODULES+=	pfl

include ${SLASHMK}

prslerr.c: ${PFL_BASE}/err.h ${SLASH_BASE}/include/slerr.h regen.pl
	${ECHORUN} perl regen.pl prslerr.c ${PFL_BASE}/err.h $( \
	    ) ${SLASH_BASE}/include/slerr.h
