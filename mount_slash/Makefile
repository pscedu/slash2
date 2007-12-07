# $Id$

include Makefile.path

PROG=		mount_slash
SRCS=		mount_slash.c
LDFLAGS+=	`pkg-config --libs fuse`
LDFLAGS+=	${LNET_LIBS} ${PFL_LIBS}
CFLAGS+=	`pkg-config --cflags fuse`

include ${SLASHMK}
