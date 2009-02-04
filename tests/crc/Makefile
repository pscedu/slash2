# $Id$

include ../Makefile.path

PROG=		crc_test
SRCS+=		crc.c
SRCS+=		${PFL_BASE}/psc_util/crc.c
INCLUDES+=	-L${SLASH_BASE}/slashd/ ${SLASH_BASE}/include/
LDFLAGS+=	${THREAD_LIBS} -lrt
CFLAGS+= -O2

include ${SLASHMK}
