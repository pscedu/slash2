# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		msctl
BIN=		msctl.sh
MAN+=		msctl.8
SRCS+=		msctl.c
SRCS+=		${SLASH_BASE}/share/ctlcli_common.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${SLASH_BASE}/share/version.c
SRCS+=		${PFL_BASE}/ctlcli.c

MODULES+=	lnet-hdrs pthread pscfs-hdrs curses clock pfl

include ${SLASHMK}
