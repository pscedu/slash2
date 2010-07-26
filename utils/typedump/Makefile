# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		typedump
SRCS+=		typedump.c
SRCS+=		${PFL_BASE}/psc_util/crc.c
SRCS+=		${SLASH_BASE}/share/slerr.c

# pthread is specified here to use correct struct sizes
MODULES+=	lnet-hdrs pthread fuse-hdrs gcrypt

include ${SLASHMK}

regen-hook:
	@${GENTYPES} -g 'SRMT_\w+|MSCMT_\w+' -h '../../*/*.h' typedump.c
