# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		typedump
SRCS+=		typedump.c
SRCS+=		${SLASH_BASE}/share/slerr.c
INCLUDES+=	-I${OBJDIR}

# pthread is specified here to use correct struct sizes
MODULES+=	lnet-hdrs pthread gcrypt pfl

include ${SLASHMK}

typedump.c: ${OBJDIR}/typedump.h

${OBJDIR}/typedump.h: ${OBJDIR} ${SLASH_BASE}/*/*.h
	${GENTYPES} -g '\b(MSCMT_\w+)\s' -g '\b(SLASH2?_\w+)\s+(?!["\s])' $(strip	\
	    ) -g '^#define\s+(\w+)\b(?=[ \t()0-9<]+\s*(?:/\*.*?\*/|)$$)' $(strip	\
	    ) -h '../../*/*.h' > $@
