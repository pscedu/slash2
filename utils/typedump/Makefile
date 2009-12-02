# $Id$

include ../Makefile.path

PROG=		typedump
SRCS+=		typedump.c
SRCS+=		${PFL_BASE}/psc_util/crc.c
INCLUDES+=	-I${LNET_BASE}/include
INCLUDES+=	-I${SLASH_BASE}/slashd
DEFINES+=	-DHAVE_LIBPTHREAD -DHAVE_LIBPTHREAD_DEFINED
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED

include ${SLASHMK}

gentypes regen:
	@${GENTYPES} -g 'SRMT_\w+|MSCMT_\w+' -h '../../*/*.h' typedump.c
