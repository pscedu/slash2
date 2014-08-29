# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		typedump
SRCS+=		typedump.c
SRCS+=		${SLASH_BASE}/share/slerr.c

# pthread is specified here to use correct struct sizes
MODULES+=	lnet-hdrs pthread gcrypt

include ${SLASHMK}

regen-hook:
	${GENTYPES} -g '\bMSCMT_\w+\s' -g '\bSLASH_\w+\s+(?!["\s])' $(	\
	    ) -g '(?<=^#define\s)(\w+)(?=\s+\d+\s*(?:/\*.*?\*/)?$$)' $(	\
	    ) -h '../../*/*.h' typedump.c
