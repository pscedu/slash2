# $Id$

include Makefile.path

PROG=		slctl
SRCS+=		slctl.c
SRCS+=		${PFL_BASE}/psc_util/ctlcli.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/strlcpy.c
DEFINES+=	-DHAVE_LIBPTHREAD_DEFINED
DEFINES+=	-DHAVE_GETHOSTBY_NAME -DHAVE_GETHOSTBYNAME_DEFINED

include ${SLASHMK}
