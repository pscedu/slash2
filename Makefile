# $Id$

SLASH_BASE=.
PROJECT_BASE=${SLASH_BASE}
include Makefile.path

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

MAN+=		doc/sladm.7
MAN+=		doc/slash.conf.5

include ${SLASHMK}

zbuild: recurse-zbuild
rezbuild: recurse-rezbuild
fullbuild: zbuild build
