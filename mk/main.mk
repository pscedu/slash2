# $Id$

INCLUDES+=		-I${SLASH_BASE}/include
INCLUDES+=		-I${SLASH_BASE}

DEFINES+=		-DAPP_STRERROR=slstrerror
SRCS+=			${SLASH_BASE}/share/slerr.c

SRC_PATH+=		$(filter-out %/tests/,$(shell ls -d ${SLASH_BASE}/*/))

SLASH_MODULES?=		cli ion mds

-include ${SLASH_BASE}/mk/local.mk

include ${MAINMK}
