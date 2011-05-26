# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slimmns_format
MAN+=		slimmns.8
SRCS+=		slimmns_format.c
SRCS+=		wipedir.c
SRCS+=		${SLASH_BASE}/share/mkfn.c
SRCS+=		${SLASH_BASE}/share/priv.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/hostname.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/random.c

SRCS+=		${SETRESUID_SRC}

MODULES+=	lnet-hdrs

include ${SLASHMK}
