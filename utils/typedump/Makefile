# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		typedump
SRCS+=		typedump.c
SRCS+=		${SLASH_BASE}/share/slerr.c

# pthread is specified here to use correct struct sizes
MODULES+=	lnet-hdrs pthread gcrypt pfl

include ${SLASHMK}

regen-hook:
	${GENTYPES} -g '\b(MSCMT_\w+)\s' -g '\b(SLASH2?_\w+)\s+(?!["\s])' $(strip	\
	    ) -g '^#define\s+(\w+)\b(?=[ \t()0-9<]+\s*(?:/\*.*?\*/|)$$)' $(strip	\
	    ) -h '../../*/*.h' typedump.c
