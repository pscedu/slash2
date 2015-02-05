# $Id$
# %PSCGPL_COPYRIGHT%

ROOTDIR=..
include ${ROOTDIR}/Makefile.path
include ${SLASH_BASE}/mk/pre.mk

MAN+=		doc/sladm.7
MAN+=		doc/slcfg.5

# this is a kludge to SRC_PATH for allowing code indexing from this dir
MODULES+=	pfl lnet-hdrs zfs

ifneq ($(filter cli,${SLASH_MODULES}),)
  SUBDIRS+=	mount_slash
  SUBDIRS+=	msctl
endif

ifneq ($(filter mds,${SLASH_MODULES}),)
  SUBDIRS+=	slashd
  SUBDIRS+=	slmctl
  SUBDIRS+=	slmkjrnl
endif

ifneq ($(filter ion,${SLASH_MODULES}),)
  SUBDIRS+=	slictl
  SUBDIRS+=	sliod
endif

ifneq ($(filter ion,${SLASH_MODULES})$(filter mds,${SLASH_MODULES}),)
  SUBDIRS+=	slmkfs
endif

SUBDIRS+=	slkeymgt

SUBDIRS+=	tests
SUBDIRS+=	utils

ifdef SLCFG
  SLCFGV=	${SLCFG}:${SLCFG_DST}
endif

include ${SLASHMK}

install-hook:
	@IFS=';' V="${SLCFGV}"; for i in $$V; do			\
		IFS=':' set -- $$i;					\
		${ECHORUN} cp -f $$1 $$2;				\
	done
