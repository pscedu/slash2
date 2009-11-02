# $Id$

include ../Makefile.path

PROG=		prslerr
SRCS+=		prslerr.c
SRCS+=		${SLASH_BASE}/share/slerr.c

include ${SLASHMK}

regen:
	@perl regen.pl prslerr.c ${SLASH_BASE}/include/slerr.h
