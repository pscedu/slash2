# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slimmns_format
MAN+=		slimmns.8
SRCS+=		slimmns_format.c
SRCS+=		wipedir.c
SRCS+=		${SLASH_BASE}/share/mkfn.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/strlcpy.c

MODULES+=	lnet-hdrs

include ${SLASHMK}
