# $Id$

include Makefile.path

PROG=		slashd
SRCS+=		fidlink.c
SRCS+=		journal.c
SRCS+=		metadir.c
INCLUDES+=	-I${LNET_BASE}/include
LDFLAGS+=	${THREAD_LIBS} ${LNET_LIBS} ${PFL_LIBS}

include ${SLASHMK}
