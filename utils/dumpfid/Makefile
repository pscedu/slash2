# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		dumpfid
FORCE_INST=	1
SRCS+=		cmp.c
SRCS+=		dumpfid.c
SRCS+=		${PFL_BASE}/crc.c
SRCS+=		${PFL_BASE}/init.c
SRCS+=		${PFL_BASE}/listcache.c
SRCS+=		${PFL_BASE}/lockedlist.c
SRCS+=		${PFL_BASE}/log.c
SRCS+=		${PFL_BASE}/pthrutil.c
SRCS+=		${PFL_BASE}/thread.c
SRCS+=		${PFL_BASE}/vbitmap.c
SRCS+=		${PFL_BASE}/waitq.c
SRCS+=		${PFL_BASE}/walk.c

MODULES+=	lnet-hdrs pfl fuse-hdrs pthread

include ${SLASHMK}
