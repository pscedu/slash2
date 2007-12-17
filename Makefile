# $Id$

SLASH_BASE=.
include Makefile.path
include ${SLASHMK}

SUBDIRS=	src

ENV=		DEFINES="-DHAVE_GETHOSTBYNAME_DEFINED -DHAVE_GETHOSTBYNAME \
		-DHAVE_LIBPTHREAD_DEFINED -DHAVE_LIBPTHREAD -DZEST_LNET"

build:
	(cd ${LNET_BASE} && ${ENV} SUBDIRS="libcfs socklnd lnet" ${MAKE} clean depend all)
	(cd ${PFL_BASE} && ${MAKE} clean depend all)
	${MAKE} clean depend all
