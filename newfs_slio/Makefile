# $Id$

include Makefile.path

PROG=		newfs_slio
SRCS+=		newfs_slio.c
SRCS+=		${SLASH_BASE}/slashd/yconf.y
SRCS+=		${SLASH_BASE}/slashd/lconf.l
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_ds/hash.c

DEFINES+=	-DHAVE_GETHOSTBYNAME -DHAVE_GETHOSTBYNAME_DEFINED
DEFINES+=	-DHAVE_LIBPTHREAD_DEFINED

include ${SLASHMK}
