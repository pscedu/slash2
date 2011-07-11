# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slmkjrnl
MAN+=		slmkjrnl.8
SRCS+=		slmkjrnl.c
SRCS+=		${SLASH_BASE}/share/mkfn.c
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${PFL_BASE}/psc_ds/dynarray.c
SRCS+=		${PFL_BASE}/psc_ds/listcache.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/crc.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/iostats.c
SRCS+=		${PFL_BASE}/psc_util/journal.c
SRCS+=		${PFL_BASE}/psc_util/log.c
SRCS+=		${PFL_BASE}/psc_util/pool.c
SRCS+=		${PFL_BASE}/psc_util/pthrutil.c
SRCS+=		${PFL_BASE}/psc_util/subsys.c
SRCS+=		${PFL_BASE}/psc_util/thread.c
SRCS+=		${PFL_BASE}/psc_util/waitq.c

MODULES+=	lnet-hdrs rt pthread aio zfs pscfs

include ${SLASHMK}

$(call ADD_FILE_CFLAGS,${PFL_BASE}/psc_util/journal.c,			\
	-DPSC_SUBSYS=SLMSS_JOURNAL -include slashd/subsys_mds.h)
