# $Id$
# %GPL_START_LICENSE%
# ---------------------------------------------------------------------
# Copyright 2015, Google, Inc.
# Copyright (c) 2007-2015, Pittsburgh Supercomputing Center (PSC).
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

ROOTDIR=..
include ${ROOTDIR}/Makefile.path
include ${SLASH_BASE}/mk/pre.mk

MAN+=		doc/sladm.7
MAN+=		doc/slcfg.5

# this is a kludge to SRC_PATH for allowing code indexing from this dir
MODULES+=	pfl lnet-hdrs zfs

ifneq ($(filter cli,${SLASH_MODULES}),)
  SUBDIRS+=	mount_slash
  SUBDIRS+=	msctl
endif

ifneq ($(filter mds,${SLASH_MODULES}),)
  SUBDIRS+=	slashd
  SUBDIRS+=	slmctl
  SUBDIRS+=	slmkjrnl
endif

ifneq ($(filter ion,${SLASH_MODULES}),)
  SUBDIRS+=	slictl
  SUBDIRS+=	sliod
endif

ifneq ($(filter ion,${SLASH_MODULES})$(filter mds,${SLASH_MODULES}),)
  SUBDIRS+=	slmkfs
endif

SUBDIRS+=	slkeymgt

SUBDIRS+=	tests
SUBDIRS+=	utils

include ${SLASHMK}
