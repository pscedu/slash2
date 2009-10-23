# $Id$

include Makefile.path

PROG=		msctl
SRCS+=		msctl.c
SRCS+=		walk.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/ctlcli.c
SRCS+=		${PFL_BASE}/psc_util/fmt.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/strlcpy.c

DEFINES+=	-DHAVE_LIBPTHREAD_DEFINED
DEFINES+=	-DHAVE_GETHOSTBY_NAME -DHAVE_GETHOSTBYNAME_DEFINED

include ${SLASHMK}
