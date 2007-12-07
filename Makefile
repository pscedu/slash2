# $Id$

SROOTDIR=.
include Makefile.path
include ${SLASHMK}

SUBDIRS=	src

ENV=		DEFINES="-DHAVE_GETHOSTBYNAME_DEFINED -DHAVE_GETHOSTBYNAME \
		-DHAVE_LIBPTHREAD_DEFINED -DHAVE_LIBPTHREAD -DZEST_LNET"

build:
	(cd ${LNET_BASE} && ${ENV} SUBDIRS="libcfs socklnd lnet" ${MAKE} clean depend all)
	(cd ${PFL_BASE} && ${MAKE} clean depend all)
	${MAKE} clean depend all

etags:
	rm -f TAGS
	find . -name \*.[chly] -exec etags -a {} \;

cscope cs:
	@cscope -Rb
