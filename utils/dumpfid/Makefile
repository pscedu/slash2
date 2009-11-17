# $Id$

include ../Makefile.path

PROG=		dumpfid
SRCS+=		dumpfid.c
SRCS+=		${PFL_BASE}/psc_util/crc.c
SRCS+=		${PFL_BASE}/psc_util/strlcat.c
DEFINES+=	-DHAVE_GETHOSTBYNAME_DEFINED=1 -DHAVE_GETHOSTBYNAME \
		-DHAVE_LIBPTHREAD_DEFINED=1

include ${SLASHMK}
