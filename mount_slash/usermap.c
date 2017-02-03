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

void
gidmap_ext_cred(struct pscfs_creds *cr)
{
	int i;
	void *p;
	struct gid_mapping *gm, q;

	if (!msl_use_mapfile)
		return;

	q.gm_key = cr->pcr_uid;
	gm = psc_hashtbl_search(&msl_gidmap_ext, &q.gm_key);
	if (gm) {
		cr->pcr_gid = gm->gm_gid;
		DYNARRAY_FOREACH(p, i, &gm->gm_gidv)
			cr->pcr_gidv[i++] = (int64_t)p;
		cr->pcr_ngid = gm->gm_ngid;
	}
}

void
uidmap_ext_cred(struct srt_creds *cr)
{
	struct uid_mapping *um, q;

	if (!msl_use_mapfile)
		return;

	q.um_key = cr->scr_uid;
	um = psc_hashtbl_search(&msl_uidmap_ext, &q.um_key);
	if (um != NULL)
		cr->scr_uid = um->um_val;
}

int
uidmap_ext_stat(struct srt_stat *sstb)
{
	struct uid_mapping *um, q;

	if (!msl_use_mapfile)
		return (0);

	q.um_key = sstb->sst_uid;
	um = psc_hashtbl_search(&msl_uidmap_ext, &q.um_key);
	if (um == NULL)
		return (0);
	sstb->sst_uid = um->um_val;
	return (0);
}

int
uidmap_int_stat(struct srt_stat *sstb)
{
	struct uid_mapping *um, q;

	if (!msl_use_mapfile)
		return (0);

	q.um_key = sstb->sst_uid;
	um = psc_hashtbl_search(&msl_uidmap_int, &q.um_key);
	if (um == NULL)
		return (0);
	sstb->sst_uid = um->um_val;
	return (0);
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
	struct uid_mapping *um;
	char *run;
	int rc = 0;

	do {
		PARSESTR(start, run);
		if (strcmp(start, "local") == 0) {
			start = run;
			local = PARSENUM(start, run);
		} else if (strcmp(start, "remote") == 0) {
			start = run;
			remote = PARSENUM(start, run);
		} else
			goto malformed;
	} while (*start);
	if (local == -1 || remote == -1)
		goto malformed;

	um = PSCALLOC(sizeof(*um));
	psc_hashent_init(&msl_uidmap_int, um);
	um->um_key = local;
	um->um_val = remote;
	psc_hashtbl_add_item(&msl_uidmap_ext, um);

	um = PSCALLOC(sizeof(*um));
	psc_hashent_init(&msl_uidmap_ext, um);
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
	struct psc_dynarray uids = DYNARRAY_INIT;
	struct gid_mapping *gm;
	int64_t remote = -1;
	int n, rc = 0;
	char *run;
	uid_t uid;
	void *p;

	do {
		PARSESTR(start, run);
		if (strcmp(start, "local-uids") == 0) {
			start = run;
			while (isdigit(*start)) {
				uid = PARSENUM(start, run);
				psc_dynarray_add(&uids,
				    (void *)(uintptr_t)uid);
			}
		} else if (strcmp(start, "remote") == 0) {
			if (remote != -1)
				goto malformed;
			start = run;
			remote = PARSENUM(start, run);
		} else
			goto malformed;
	} while (*start);
	if (psc_dynarray_len(&uids) == 0 || remote == -1)
		goto malformed;

	DYNARRAY_FOREACH(p, n, &uids) {
		gm = psc_hashtbl_search(&msl_gidmap_ext, p);
		if (gm)
			psc_dynarray_add(&gm->gm_gidv, (void *)remote);
		else {
			gm = PSCALLOC(sizeof(*gm));
			psc_hashent_init(&msl_gidmap_ext, gm);
			gm->gm_key = (uint64_t)p;
			gm->gm_gid = remote;
			psc_hashtbl_add_item(&msl_gidmap_ext, gm);
		}
	}
	rc = 1;

 malformed:
	psc_dynarray_free(&uids);
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
		 * Skip comments that starts with # sign.
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
		else if (strcmp(start, "group") == 0 &&
		    mapfile_parse_group(run)) {
			good++;
			continue;
		}

 malformed:
		if (*start == '\0')
			continue;

		warnx("%s: %d: malformed line", fn, ln);
	}
	if (ferror(fp))
		errx(1, "I/O error on map file %s.", fn);
	fclose(fp);
	if (!good)
		errx(1, "Map file %s is empty.", fn);
	warnx("%d entries in the map file %s have been parsed successfully.", 
	    good, fn);
}
