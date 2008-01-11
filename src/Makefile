# $Id$

include Makefile.path

PROG=		slashd
#SRCS+=		fidlink.c
SRCS+=		journal.c
SRCS+=		lconf.l
SRCS+=		main.c
SRCS+=		mds.c
#SRCS+=		metadir.c
SRCS+=		slash_appthread.c
SRCS+=		yconf.y
INCLUDES+=	-I${LNET_BASE}/include
LDFLAGS+=	${THREAD_LIBS} ${LNET_LIBS} ${PFL_LIBS}
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD -DHAVE_LIBPTHREAD_DEFINED

include ${SLASHMK}
