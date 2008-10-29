# $Id$

include ${SLASH_BASE}/mk/local.mk

INCLUDES+=	-I${SLASH_BASE}/include -I.
INCLUDES+=	-I${PFL_BASE}/include
INCLUDES+=	-I${LNET_BASE}/include
DEFINES+=	-DYY_NO_UNPUT
CFLAGS+=	-Wall -W -g ${INCLUDES} ${DEFINES}
#CFLAGS+=	-Wshadow -Wunused -Wuninitialized -O
YFLAGS+=	-d -o $@

# Default to build a binary, but may be overridden after
# this file has been included, e.g. for a ${LIBRARY}.
TARGET?=	${PROG}

include ${MAINMK}
