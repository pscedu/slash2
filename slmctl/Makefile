# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slmctl
MAN+=		slmctl.8
SRCS+=		slmctl.c
SRCS+=		${SLASH_BASE}/share/ctlcli_common.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${SLASH_BASE}/share/version.c
SRCS+=		${PFL_BASE}/ctlcli.c

DEFINES+=	-D_SLASH_MDS
MODULES+=	lnet-hdrs pthread curses pfl

include ${SLASHMK}
