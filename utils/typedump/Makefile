# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		typedump
SRCS+=		typedump.c
SRCS+=		${PFL_BASE}/psc_util/crc.c

# pthread is specified here to use correct struct sizes
MODULES+=	lnet-hdrs pthread

include ${SLASHMK}

gentypes regen:
	@${GENTYPES} -g 'SRMT_\w+|MSCMT_\w+' -h '../../*/*.h' typedump.c
