# $Id$

include ../Makefile.path

PROG=		slconfig_test
SRCS+=		slconfig_test.c
SRCS+=		${SLASH_BASE}/src/lconf.l
SRCS+=		${SLASH_BASE}/src/yconf.y
LDFLAGS+=	${PFL_LIBS} ${LIBL} ${LNET_LIBS}
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD_DEFINED

include ${SLASHMK}
