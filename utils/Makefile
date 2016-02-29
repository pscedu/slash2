# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path
include ${SLASH_BASE}/mk/pre.mk

BIN+=		slash2_check
BIN+=		slash2_check.py
INSTDIR=	${INST_SBINDIR}
SUBDIRS+=	prslerr
SUBDIRS+=	typedump

ifneq ($(filter zfs,${SLASH_MODULES}),)
  BIN+=		zfs-error.py
  BIN+=		ssh.py
  SUBDIRS+=	cursor
  SUBDIRS+=	dumpfid
  SUBDIRS+=	reclaim
endif

ifneq ($(filter ion,${SLASH_MODULES}),)
  BIN+=		file_lookup.py
  BIN+=		nsliods
  BIN+=		slictlN
  BIN+=		fshealthtest
endif

include ${SLASHMK}
