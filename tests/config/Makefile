# $Id$

include ../Makefile.path

PROG=		slconfig_test
SRCS+=		slconfig_test.c
SRCS+=		${SLASH_BASE}/src/lconf.l
SRCS+=		${SLASH_BASE}/src/yconf.y
LDFLAGS+=	${LIBL} ${LNET_LIBS} -lpthread ${PFL_LIBS}
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD_DEFINED -DPSC_LNET=1

include ${SLASHMK}
