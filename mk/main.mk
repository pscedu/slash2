# $Id$

include ${SROOTDIR}/mk/local.mk

INCLUDES+=	-I${PFL_BASE}/include
INCLUDES+=	-I${SROOTDIR}/include -I.
CFLAGS+=	-Wall -W -g ${INCLUDES} ${DEFINES}
CFLAGS+=	-DYY_NO_UNPUT
#CFLAGS+=	-Wshadow -Wunused -Wuninitialized -O
YFLAGS+=	-d -o $@

# Default to build a binary, but may be overridden after
# this file has been included, e.g. for a ${LIBRARY}.
TARGET?=	${PROG}

include ${MAINMK}
