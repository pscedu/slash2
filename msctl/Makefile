# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		msctl
MAN+=		msctl.8
SRCS+=		msctl.c
SRCS+=		${SLASH_BASE}/share/ctlcli_common.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${PFL_BASE}/alloc.c
SRCS+=		${PFL_BASE}/ctlcli.c
SRCS+=		${PFL_BASE}/dynarray.c
SRCS+=		${PFL_BASE}/fmt.c
SRCS+=		${PFL_BASE}/hashtbl.c
SRCS+=		${PFL_BASE}/init.c
SRCS+=		${PFL_BASE}/log.c
SRCS+=		${PFL_BASE}/pthrutil.c
SRCS+=		${PFL_BASE}/subsys.c
SRCS+=		${PFL_BASE}/thread.c
SRCS+=		${PFL_BASE}/vbitmap.c
SRCS+=		${PFL_BASE}/waitq.c
SRCS+=		${PFL_BASE}/walk.c

MODULES+=	lnet-hdrs pthread pscfs-hdrs curses clock

include ${SLASHMK}
