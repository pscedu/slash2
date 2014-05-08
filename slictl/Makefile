# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slictl
MAN+=		slictl.8
SRCS+=		slictl.c
SRCS+=		${SLASH_BASE}/share/ctlcli_common.c
SRCS+=		${SLASH_BASE}/share/slerr.c

MODULES+=	lnet-hdrs pthread clock pfl curses

include ${SLASHMK}
