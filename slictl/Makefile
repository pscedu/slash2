# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slictl
MAN+=		slictl.8
SRCS+=		slictl.c
SRCS+=		${SLASH_BASE}/share/ctlcli_common.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${SLASH_BASE}/share/version.c
SRCS+=		${PFL_BASE}/ctlcli.c

MODULES+=	lnet-hdrs pthread clock pfl curses

include ${SLASHMK}
