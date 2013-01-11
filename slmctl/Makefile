# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slmctl
MAN+=		slmctl.8
SRCS+=		slmctl.c
SRCS+=		${SLASH_BASE}/share/ctlcli_common.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${PFL_BASE}/alloc.c
SRCS+=		${PFL_BASE}/ctlcli.c
SRCS+=		${PFL_BASE}/dynarray.c
SRCS+=		${PFL_BASE}/fmt.c
SRCS+=		${PFL_BASE}/init.c
SRCS+=		${PFL_BASE}/log.c
SRCS+=		${PFL_BASE}/pthrutil.c
SRCS+=		${PFL_BASE}/subsys.c
SRCS+=		${PFL_BASE}/thread.c
SRCS+=		${PFL_BASE}/waitq.c

DEFINES+=	-D_SLASH_MDS
MODULES+=	lnet-hdrs pthread curses

include ${SLASHMK}
