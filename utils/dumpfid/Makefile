# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		dumpfid
FORCE_INST=	1
SRCS+=		dumpfid.c
SRCS+=		${PFL_BASE}/psc_util/crc.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/log.c

MODULES+=	lnet-hdrs pfl fuse-hdrs

include ${SLASHMK}
