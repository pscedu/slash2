# $Id$

include Makefile.path

PROG=		slkeymgt
MAN+=		slkeymgt.8
SRCS+=		slkeymgt.c
SRCS+=		${SLASH_BASE}/share/fdbuf.c
SRCS+=		${SLASH_BASE}/share/mkfn.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/base64.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/random.c
SRCS+=		${PFL_BASE}/psc_util/strlcpy.c
LDFLAGS+=	-lgcrypt

MODULES+=	lnet-hdrs

include ${SLASHMK}
