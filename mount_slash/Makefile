# $Id$

include Makefile.path

PROG=		mount_slash
SRCS=		mount_slash.c
SRCS+=		rpc.c
LDFLAGS+=	`pkg-config --libs fuse`
LDFLAGS+=	${LNET_LIBS} ${PFL_LIBS}
CFLAGS+=	`pkg-config --cflags fuse`
INCLUDES+=	-I${LNET_BASE}/include
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD_DEFINED

include ${SLASHMK}
