/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2015, Pittsburgh Supercomputing Center (PSC).
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include "pfl/hashtbl.h"

#include "mkfn.h"
#include "mount_slash.h"
#include "pathnames.h"


/*
 * Perform UID and GID mapping when a map file is given.
 */

/* Externalize UID and GID credential for permission checking */

void
uidmap_ext_cred(struct pscfs_creds *cr)
{
	struct uid_mapping *um, q;

	if (!msl_map_enable)
		return;

	q.um_key = cr->pcr_uid;
	um = psc_hashtbl_search(&msl_uidmap_ext, &q.um_key);
	if (um)
		cr->pcr_uid = um->um_val;
	else {
		/* uid squashing */
		cr->pcr_uid = 65534;
	}
}

void
gidmap_ext_cred(struct pscfs_creds *cr)
{
	int i, j;
	gid_t gid;
	struct gid_mapping *gm, q;

	if (!msl_map_enable)
		return;

	q.gm_key = cr->pcr_gid;
	gm = psc_hashtbl_search(&msl_gidmap_ext, &q.gm_key);
	if (gm)
		gid = gm->gm_val;
	else 
		gid = 65534;
	j = 0;
	for (i = 0; i < cr->pcr_ngid; i++) {
		if (cr->pcr_gid == cr->pcr_gidv[i]) {
			cr->pcr_gidv[j] = gid;
			j++;
			continue;
		}
		q.gm_key = cr->pcr_gidv[i];
		gm = psc_hashtbl_search(&msl_gidmap_ext, &q.gm_key);
		if (!gm)
			continue;
		/* overwrite is fine because i >= j */
		cr->pcr_gidv[j] = gm->gm_val;
		j++;
	}
	cr->pcr_gid = gid;
	cr->pcr_ngid = j;
}

/* Externalize UID and GID credential for setting attributes */

void
uidmap_ext_stat(struct stat *stb)
{
	struct uid_mapping *um, q;

	if (!msl_map_enable)
		return;

	q.um_key = stb->st_uid;
	um = psc_hashtbl_search(&msl_uidmap_ext, &q.um_key);
	if (um)
		stb->st_uid = um->um_val;
	else
		stb->st_uid = 65534;
}

void
gidmap_ext_stat(struct stat *stb)
{
	struct gid_mapping *gm, q;

	if (!msl_map_enable)
		return;

	q.gm_key = stb->st_gid;
	gm = psc_hashtbl_search(&msl_gidmap_ext, &q.gm_key);
	if (gm)
		stb->st_gid = gm->gm_val;
	else
		stb->st_gid = 65534;
}

/* Internalize UID and GID credential for attribute reporting to FUSE */

void
uidmap_int_stat(struct srt_stat *sstb, uint32_t *uidp)
{
	uint32_t uid;
	struct uid_mapping *um, q;

	uid = sstb->sst_uid;
	if (msl_map_enable) {
		q.um_key = sstb->sst_uid;
		um = psc_hashtbl_search(&msl_uidmap_int, &q.um_key);
		if (um)
			uid = um->um_val;
		else
			uid = 65534;
	}
	*uidp = uid;
}

void
gidmap_int_stat(struct srt_stat *sstb, uint32_t *gidp)
{
	uint32_t gid;
	struct gid_mapping *gm, q;

	gid = sstb->sst_gid;
	if (msl_map_enable) {
		q.gm_key = sstb->sst_gid;
		gm = psc_hashtbl_search(&msl_gidmap_int, &q.gm_key);
		if (gm)
			gid = gm->gm_val;
		else
			gid = 65534;
	}
	*gidp = gid;
}

#define PARSESTR(start, run)						\
	do {								\
		for ((run) = (start); isalpha(*run) || *(run) == '-';	\
		    (run)++)						\
			;						\
		if (!isspace(*(run)))					\
			goto malformed;					\
		*(run)++ = '\0';					\
		while (isspace(*(run)))					\
			(run)++;					\
	} while (0)

#define PARSENUM(start, run)						\
	_PFL_RVSTART {							\
		char *_endp;						\
		long _l;						\
									\
		for ((run) = (start); isdigit(*(run)); (run)++)		\
			;						\
		if (!isspace(*(run)))					\
			goto malformed;					\
		*(run)++ = '\0';					\
		while (isspace(*(run)))					\
			(run)++;					\
		_l = strtol((start), &_endp, 10);			\
		if (_l < 0 || _l >= INT_MAX ||				\
		    _endp == (start) || *_endp)				\
			goto malformed;					\
		(start) = (run);					\
		(_l);							\
	} _PFL_RVEND

int
mapfile_parse_user(char *start)
{
	int64_t local = -1, remote = -1;
	struct uid_mapping *um, q;
	char *run;
	int rc = 0;

	do {
		/* the order of local and remote does not matter */
		PARSESTR(start, run);
		if (strcmp(start, "local") == 0) {
			start = run;
			local = PARSENUM(start, run);
			continue;
		}
		if (strcmp(start, "remote") == 0) {
			start = run;
			remote = PARSENUM(start, run);
			continue;
		}
		goto malformed;

	} while (*start);

	if (local == -1 || remote == -1)
		goto malformed;

	q.um_key = local;
	um = psc_hashtbl_search(&msl_uidmap_ext, &q.um_key);
	if (um != NULL)
		goto malformed;

	q.um_key = remote;
	um = psc_hashtbl_search(&msl_uidmap_int, &q.um_key);
	if (um != NULL)
		goto malformed;

	um = PSCALLOC(sizeof(*um));
	psc_hashent_init(&msl_uidmap_ext, um);
	um->um_key = local;
	um->um_val = remote;
	psc_hashtbl_add_item(&msl_uidmap_ext, um);

	um = PSCALLOC(sizeof(*um));
	psc_hashent_init(&msl_uidmap_int, um);
	um->um_key = remote;
	um->um_val = local;
	psc_hashtbl_add_item(&msl_uidmap_int, um);
	rc = 1;

 malformed:

	return (rc);
}

int
mapfile_parse_group(char *start)
{
	int64_t local = -1, remote = -1;
	struct gid_mapping *gm, q;
	char *run;
	int rc = 0, localfirst = 0;

	do {
		/* the order of local and remote does matter */
		PARSESTR(start, run);
		if (strcmp(start, "local") == 0) {
			if (remote == -1)
				localfirst = 1;
			start = run;
			local = PARSENUM(start, run);
			continue;
		}
		if (strcmp(start, "remote") == 0) {
			start = run;
			remote = PARSENUM(start, run);
			continue;
		} 
		goto malformed;

	} while (*start);

	if (local == -1 || remote == -1)
		goto malformed;

	if (localfirst) {
		q.gm_key = local;
		gm = psc_hashtbl_search(&msl_gidmap_ext, &q.gm_key);
		if (gm != NULL)
			goto malformed;

		gm = PSCALLOC(sizeof(*gm));
		psc_hashent_init(&msl_gidmap_ext, gm);
		gm->gm_key = local;
		gm->gm_val = remote;
		psc_hashtbl_add_item(&msl_gidmap_ext, gm);
	} else {
		q.gm_key = remote;
		gm = psc_hashtbl_search(&msl_gidmap_int, &q.gm_key);
		if (gm != NULL)
			goto malformed;

		gm = PSCALLOC(sizeof(*gm));
		psc_hashent_init(&msl_gidmap_int, gm);
		gm->gm_key = remote;
		gm->gm_val = local;
		psc_hashtbl_add_item(&msl_gidmap_int, gm);
	}
	rc = 1;

 malformed:

	return (rc);
}

void
parse_mapfile(void)
{
	char fn[PATH_MAX], buf[LINE_MAX], *start, *run;
	FILE *fp;
	int ln, good;

	xmkfn(fn, "%s/%s", sl_datadir, SL_FN_MAPFILE);

	fp = fopen(fn, "r");
	if (fp == NULL)
		errx(1, "Fail to open map file %s.", fn);
	ln = good = 0;
	while (fgets(buf, sizeof(buf), fp)) {
		ln++;

		/*
		 * Skip comments that start with # sign.
		 */
		start = buf;
		if (*start == '#')
		    continue;

		/*
		 * There must be at least one space after
		 * either "user" or "group" string.
		 */
		PARSESTR(start, run);
		if (strcmp(start, "user") == 0 &&
		    mapfile_parse_user(run)) {
			good++;
			continue;
		}
		if (strcmp(start, "group") == 0 &&
		    mapfile_parse_group(run)) {
			good++;
			continue;
		}

 malformed:
		if (*start != '\0')
			errx(1, "%s: %d: malformed or duplicate line", fn, ln);
	}
	if (ferror(fp))
		errx(1, "I/O error on map file %s.", fn);
	fclose(fp);
	if (!good)
		errx(1, "Map file %s is empty.", fn);
	warnx("%d entries in the map file %s have been parsed successfully.", 
	    good, fn);
}
