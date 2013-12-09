# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		dumpfid
FORCE_INST=	1
SRCS+=		dumpfid.c
SRCS+=		${PFL_BASE}/crc.c
SRCS+=		${PFL_BASE}/init.c
SRCS+=		${PFL_BASE}/lockedlist.c
SRCS+=		${PFL_BASE}/log.c
SRCS+=		${PFL_BASE}/walk.c

MODULES+=	lnet-hdrs pfl fuse-hdrs

include ${SLASHMK}
