# $Id$

include Makefile.path

PROG=		msctl
SRCS+=		msctl.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/ctlcli.c
DEFINES+=	-DHAVE_LIBPTHREAD_DEFINED
DEFINES+=	-DHAVE_GETHOSTBY_NAME -DHAVE_GETHOSTBYNAME_DEFINED

include ${SLASHMK}
