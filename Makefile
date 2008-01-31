# $Id$

SLASH_BASE=.
PROJECT_BASE=${SLASH_BASE}
include Makefile.path
include ${SLASHMK}

SUBDIRS=	slashd
SUBDIRS+=	sliod
SUBDIRS+=	slctl
SUBDIRS+=	mount_slash

ENV=		DEFINES="-DHAVE_GETHOSTBYNAME_DEFINED -DHAVE_GETHOSTBYNAME \
		-DHAVE_LIBPTHREAD_DEFINED -DHAVE_LIBPTHREAD -DPSC_LNET"

build:
	(cd ${LNET_BASE} && ${ENV} SUBDIRS="libcfs socklnd lnet" ${MAKE} clean depend all)
	${MAKE} clean depend all
