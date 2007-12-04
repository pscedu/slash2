# $Id$

SROOTDIR=.
include Makefile.path
include ${SLASHMK}

SUBDIRS=	src

build:
	(cd ${LNET_BASE} && ${MAKE} clean depend all)
	(cd ${PFL_BASE} && ${MAKE} clean depend all)
	${MAKE} clean depend all

etags:
	rm -f TAGS
	find . -name \*.[chly] -exec etags -a {} \;

cscope cs:
	@cscope -Rb
