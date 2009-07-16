# $Id$

include ../Makefile.path

PROG=		typedump
SRCS=		typedump.c
#SRCS+=		${PFL_BASE}/psc_util/log.c
#SRCS+=		${PFL_BASE}/psc_util/alloc.c
INCLUDES+=	-I${LNET_BASE}/include
INCLUDES+=	-I${SLASH_BASE}/slashd
DEFINES+=	-DHAVE_LIBPTHREAD -DHAVE_LIBPTHREAD_DEFINED
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED

include ${SLASHMK}

gentypes:
	@${GENTYPES} -h '../../*/*.h' typedump.c
