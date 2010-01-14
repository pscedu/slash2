# $Id$

include ../Makefile.path

PROG=		slconfig_test
SRCS+=		slconfig_test.c
SRCS+=		${SLASH_BASE}/share/cfg_common.c
SRCS+=		${SLASH_BASE}/share/lconf.l
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${SLASH_BASE}/share/yconf.y
SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/hash2.c
SRCS+=		${PFL_BASE}/psc_ds/vbitmap.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/iostats.c
SRCS+=		${PFL_BASE}/psc_util/list.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/pthrutil.c
SRCS+=		${PFL_BASE}/psc_util/strlcpy.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/usklndthr.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c

SRCS+=		${LNET_SOCKLND_SRCS}
SRCS+=		${PSCRPC_SRCS}

LDFLAGS+=	${LIBL}
MODULES+=	lnet pthread

include ${SLASHMK}
