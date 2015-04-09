/* $Id$ */
/* %PSCGPL_COPYRIGHT% */

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#include "mount_slash.sh"
#include "pathnames.sh"

int
uidmap_ext_cred(struct srt_creds *cr)
{
	struct uid_mapping *um, q;

	if (!use_mapfile)
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

	if (!use_mapfile)
		return (0);

	q.gm_key = cr->pcr_gid;
	gm = psc_hashtbl_search(&slc_gidmap_int, NULL, NULL, &q.gm_key);
	if (gm == NULL)
		return (0);
	cr->pcr_gid = gm->gm_val;
	memcpy(cr->pcr_gidv, gm->gm_gidv, sizeof(gm->gm_gidv));
	return (0);
}

int
uidmap_ext_stat(struct srt_stat *sstb)
{
	struct uid_mapping *um, q;

	if (!use_mapfile)
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

	if (!use_mapfile)
		return (0);

	q.um_key = sstb->sst_uid;
	um = psc_hashtbl_search(&slc_uidmap_int, NULL, NULL, &q.um_key);
	if (um == NULL)
		return (0);
	sstb->sst_uid = um->um_val;
	return (0);
}

void
parse_mapfile(void)
{
	char fn[PATH_MAX], buf[LINE_MAX], *endp, *p, *t;
	struct uid_mapping *um;
	uid_t to, from;
	FILE *fp;
	long l;
	int ln;

	xmkfn(fn, "%s/%s", sl_datadir, SL_FN_MAPFILE);

	fp = fopen(fn, "r");
	if (fp == NULL)
		err(1, "%s", fn);
	ln = 0;
	while (fgets(buf, sizeof(buf), fp)) {
		ln++;

		for (p = t = buf; isdigit(*t); t++)
			;
		if (!isspace(*t))
			goto malformed;
		*t++ = '\0';
		while (isspace(*t))
			t++;

		l = strtol(p, &endp, 10);
		if (l < 0 || l >= INT_MAX || endp == p || *endp)
			goto malformed;
		from = l;

		for (p = t; isdigit(*t); t++)
			;
		*t++ = '\0';
		while (isspace(*t))
			t++;
		if (*t)
			goto malformed;

		l = strtol(p, &endp, 10);
		if (l < 0 || l >= INT_MAX || endp == p || *endp)
			goto malformed;
		to = l;

		um = PSCALLOC(sizeof(*um));
		psc_hashent_init(&slc_uidmap_ext, um);
		um->um_key = from;
		um->um_val = to;
		psc_hashtbl_add_item(&slc_uidmap_ext, um);

		um = PSCALLOC(sizeof(*um));
		psc_hashent_init(&slc_uidmap_int, um);
		um->um_key = to;
		um->um_val = from;
		psc_hashtbl_add_item(&slc_uidmap_int, um);

		continue;

 malformed:
		warn("%s: %d: malformed line", fn, ln);
	}
	if (ferror(fp))
		warn("%s", fn);
	fclose(fp);
}
