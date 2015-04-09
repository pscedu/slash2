/* $Id$ */
/* %PSCGPL_COPYRIGHT% */

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#include "pfl/hashtbl.h"

#include "mkfn.h"
#include "mount_slash.h"
#include "pathnames.h"

int
uidmap_ext_cred(struct srt_creds *cr)
{
	struct uid_mapping *um, q;

	if (!slc_use_mapfile)
		return (0);

	q.um_key = cr->scr_uid;
	um = psc_hashtbl_search(&slc_uidmap_ext, NULL, NULL, &q.um_key);
	if (um == NULL)
		return (0);
	cr->scr_uid = um->um_val;
	return (0);
}

int
gidmap_int_cred(struct pscfs_creds *cr)
{
	struct gid_mapping *gm, q;

	if (!slc_use_mapfile)
		return (0);

	q.gm_key = cr->pcr_gid;
	gm = psc_hashtbl_search(&slc_gidmap_int, NULL, NULL, &q.gm_key);
	if (gm == NULL)
		return (0);
	cr->pcr_gid = gm->gm_gid;
	memcpy(cr->pcr_gidv, gm->gm_gidv, sizeof(gm->gm_gidv));
	return (0);
}

int
uidmap_ext_stat(struct srt_stat *sstb)
{
	struct uid_mapping *um, q;

	if (!slc_use_mapfile)
		return (0);

	q.um_key = sstb->sst_uid;
	um = psc_hashtbl_search(&slc_uidmap_ext, NULL, NULL, &q.um_key);
	if (um == NULL)
		return (0);
	sstb->sst_uid = um->um_val;
	return (0);
}

int
uidmap_int_stat(struct srt_stat *sstb)
{
	struct uid_mapping *um, q;

	if (!slc_use_mapfile)
		return (0);

	q.um_key = sstb->sst_uid;
	um = psc_hashtbl_search(&slc_uidmap_int, NULL, NULL, &q.um_key);
	if (um == NULL)
		return (0);
	sstb->sst_uid = um->um_val;
	return (0);
}

#define PARSESTR(start, run)						\
	do {								\
		for ((run) = (start); isalpha(*run); (run)++)		\
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
	psc_hashent_init(&slc_uidmap_ext, um);
	um->um_key = local;
	um->um_val = remote;
	psc_hashtbl_add_item(&slc_uidmap_ext, um);

	um = PSCALLOC(sizeof(*um));
	psc_hashent_init(&slc_uidmap_int, um);
	um->um_key = remote;
	um->um_val = local;
	psc_hashtbl_add_item(&slc_uidmap_int, um);

	return (1);
 malformed:
	return (0);
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
				    (void *)(unsigned long)uid);
			}
		} else if (strcmp(start, "remote") == 0) {
			start = run;
			remote = PARSENUM(start, run);
		} else
			goto malformed;
	} while (*start);
	if (psc_dynarray_len(&uids) == 0 || remote == -1)
		goto malformed;

	DYNARRAY_FOREACH(p, n, &uids) {
		gm = psc_hashtbl_search(&slc_gidmap_int, NULL, NULL,
		    &remote);
		if (gm) {
			gm->gm_gidv[gm->gm_ngid++] = remote;
		} else {
			gm = PSCALLOC(sizeof(*gm));
			psc_hashent_init(&slc_gidmap_int, gm);
			gm->gm_key = (uint64_t)p;
			gm->gm_gid = remote;
			psc_hashtbl_add_item(&slc_gidmap_int, gm);
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
	int ln;

	xmkfn(fn, "%s/%s", sl_datadir, SL_FN_MAPFILE);

	fp = fopen(fn, "r");
	if (fp == NULL)
		err(1, "%s", fn);
	ln = 0;
	while (fgets(buf, sizeof(buf), fp)) {
		ln++;

		start = buf;
		PARSESTR(start, run);
		if (strcmp(start, "user") == 0 &&
		    mapfile_parse_user(start))
			continue;
		else if (strcmp(start, "group") &&
		    mapfile_parse_group(start))
			continue;

 malformed:
		warn("%s: %d: malformed line", fn, ln);
	}
	if (ferror(fp))
		warn("%s", fn);
	fclose(fp);
}
