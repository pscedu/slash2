# $Id$

include Makefile.path

PROG=		mount_slash
SRCS=		mount_slash.c
LDFLAGS+=	`pkg-config --libs fuse`
LDFLAGS+=	${LNET_LIBS}

include ${SLASHMK}
