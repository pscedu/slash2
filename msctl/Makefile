# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		msctl
MAN+=		msctl.8
SRCS+=		msctl.c
SRCS+=		${SLASH_BASE}/share/ctlcli_common.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hashtbl.c
SRCS+=		${PFL_BASE}/psc_ds/vbitmap.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/ctlcli.c
SRCS+=		${PFL_BASE}/psc_util/fmt.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/pthrutil.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c
SRCS+=		${PFL_BASE}/psc_util/walk.c

MODULES+=	lnet-hdrs pthread pscfs-hdrs curses clock

include ${SLASHMK}
