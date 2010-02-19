# $Id$

include ${SLASH_BASE}/mk/local.mk

INCLUDES+=	-I${PFL_BASE}/include
INCLUDES+=	-I${SLASH_BASE}/include
INCLUDES+=	-I${SLASH_BASE}

DEFINES+=	-DAPP_STRERROR=slstrerror

# Add additional flags here
#DEFINES+=	-DNAMESPACE_EXPERIMENTAL

include ${MAINMK}
