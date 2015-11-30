# $Id$
# %GPL_START_LICENSE%
# ---------------------------------------------------------------------
# Copyright 2015, Google, Inc.
# Copyright (c) 2015, Pittsburgh Supercomputing Center (PSC).
# All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or (at
# your option) any later version.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
# PURPOSE.  See the GNU General Public License contained in the file
# `COPYING-GPL' at the top of this distribution or at
# https://www.gnu.org/licenses/gpl-2.0.html for more details.
# ---------------------------------------------------------------------
# %END_LICENSE%

ifndef INCL_SLASH2_PRE_MK
INCL_SLASH2_PRE_MK=1

INCLUDES+=		-I${SLASH_BASE}/include
INCLUDES+=		-I${SLASH_BASE}
INCLUDES+=		${SQLITE3_INCLUDES}

DEFINES+=		-DSL_STK_VERSION=$$(git log | grep -c ^commit)
SRCS+=			${SLASH_BASE}/share/slerr.c

SRC_PATH+=		${SLASH_BASE}/include
SRC_PATH+=		${SLASH_BASE}/mount_slash
SRC_PATH+=		${SLASH_BASE}/share
SRC_PATH+=		${SLASH_BASE}/slashd
SRC_PATH+=		${SLASH_BASE}/sliod

SLASH_MODULES?=		cli ion mds

-include ${SLASH_BASE}/mk/local.mk

ifneq ($(filter mds,${SLASH_MODULES}),)
 SLASH_MODULES+=	zfs
endif

ifeq (${CURDIR},$(realpath ${SLASH_BASE}/mount_slash))
 ifneq ($(filter acl,${SLASH_OPTIONS}),)
  SRCS+=		${SLASH_BASE}/mount_slash/acl_cli.c
  DEFINES+=		-DSLOPT_POSIX_ACLS
  DEFINES+=		-DSLOPT_POSIX_ACLS_REVERT
  LDFLAGS+=		-lacl
 endif
endif

ifneq ($(filter module,${SLASH_OPTIONS}),)
  DEFINES+=		-DMSL_PFLFS_MODULE
endif

endif
