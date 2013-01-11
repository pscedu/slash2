# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

TEST=		slconfig_test
SRCS+=		slconfig_test.c
SRCS+=		${SLASH_BASE}/share/cfg_common.c
SRCS+=		${SLASH_BASE}/share/lconf.l
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${SLASH_BASE}/share/yconf.y
SRCS+=		${PFL_BASE}/dynarray.c
SRCS+=		${PFL_BASE}/hashtbl.c
SRCS+=		${PFL_BASE}/vbitmap.c
SRCS+=		${PFL_BASE}/alloc.c
SRCS+=		${PFL_BASE}/init.c
SRCS+=		${PFL_BASE}/iostats.c
SRCS+=		${PFL_BASE}/list.c
SRCS+=		${PFL_BASE}/log.c
SRCS+=		${PFL_BASE}/netutil.c
SRCS+=		${PFL_BASE}/pthrutil.c
SRCS+=		${PFL_BASE}/subsys.c
SRCS+=		${PFL_BASE}/thread.c
SRCS+=		${PFL_BASE}/usklndthr.c
SRCS+=		${PFL_BASE}/waitq.c

MODULES+=	lnet pthread rpc clock
DEFINES+=	-D_SLASH_CLIENT

include ${SLASHMK}
