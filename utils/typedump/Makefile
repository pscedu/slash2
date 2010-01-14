# $Id$

include ../Makefile.path

PROG=		typedump
SRCS+=		typedump.c
SRCS+=		${PFL_BASE}/psc_util/crc.c

MODULES+=	lnet-hdrs

include ${SLASHMK}

gentypes regen:
	@${GENTYPES} -g 'SRMT_\w+|MSCMT_\w+' -h '../../*/*.h' typedump.c
