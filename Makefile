# $Id$

ROOTDIR=..
include ${ROOTDIR}/Makefile.path

MAN+=		doc/sladm.7
MAN+=		doc/slash.conf.5

include ${SLASHMK}

ifneq ($(filter cli,${SLASH_MODULES}),)
SUBDIRS+=	mount_slash
SUBDIRS+=	msctl
endif

ifneq ($(filter mds,${SLASH_MODULES}),)
SUBDIRS+=	slashd
SUBDIRS+=	slmctl
endif

ifneq ($(filter ion,${SLASH_MODULES}),)
SUBDIRS+=	slictl
SUBDIRS+=	sliod
endif

ifneq ($(filter ion,${SLASH_MODULES})$(filter mds,${SLASH_MODULES}),)
SUBDIRS+=	slimmns
SUBDIRS+=	slkeymgt
SUBDIRS+=	slmkjrnl
endif

zbuild:
	@(cd slashd && ${MAKE} zbuild)

rezbuild:
	@(cd slashd && ${MAKE} rezbuild)

fullbuild: zbuild build
