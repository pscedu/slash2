# $Id$

ROOTDIR=..
include ${ROOTDIR}/Makefile.path

MAN+=		doc/sladm.7
MAN+=		doc/slcfg.5

# this is a workaround to SRC_PATH better
MODULES+=	pfl lnet-hdrs zfs

include ${SLASHMK}

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

install-hook:
	@IFS=';' V="${SLCFGV}"; for i in $$V; do			\
		IFS=':' set -- $$i;					\
		${ECHORUN} cp -f $$1 $$2;				\
	done
