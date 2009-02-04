# $Id$

include ${SLASH_BASE}/mk/local.mk

INCLUDES+=	-I${PFL_BASE}/include
INCLUDES+=	-I${SLASH_BASE}/include
INCLUDES+=	-I${SLASH_BASE} -I.

CFLAGS+=	${INCLUDES} ${DEFINES}
YFLAGS+=	-d -o $@

# Default to build a binary, but may be overridden after
# this file has been included, e.g. for a ${LIBRARY}.
TARGET?=	${PROG}

include ${MAINMK}
