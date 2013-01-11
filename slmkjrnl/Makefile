# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slmkjrnl
MAN+=		slmkjrnl.8
SRCS+=		slmkjrnl.c
SRCS+=		${SLASH_BASE}/share/mkfn.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${PFL_BASE}/alloc.c
SRCS+=		${PFL_BASE}/crc.c
SRCS+=		${PFL_BASE}/dynarray.c
SRCS+=		${PFL_BASE}/hashtbl.c
SRCS+=		${PFL_BASE}/init.c
SRCS+=		${PFL_BASE}/iostats.c
SRCS+=		${PFL_BASE}/listcache.c
SRCS+=		${PFL_BASE}/log.c
SRCS+=		${PFL_BASE}/pool.c
SRCS+=		${PFL_BASE}/pthrutil.c
SRCS+=		${PFL_BASE}/subsys.c
SRCS+=		${PFL_BASE}/thread.c
SRCS+=		${PFL_BASE}/waitq.c

MODULES+=	lnet-hdrs rt pthread aio pscfs

include ${SLASHMK}
