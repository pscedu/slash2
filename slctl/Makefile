# $Id$

include Makefile.path

PROG=		slctl
SRCS+=		slctl.c
SRCS+=		${SLASH_BASE}/share/str.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/ctlcli.c
SRCS+=		${PFL_BASE}/psc_util/fmt.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/strlcpy.c

INCLUDES+=	-I${LNET_BASE}/include
DEFINES+=	-DHAVE_LIBPTHREAD_DEFINED
DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED

include ${SLASHMK}
