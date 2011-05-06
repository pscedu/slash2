/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2011, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

%{
#define YYSTYPE char *

#include <ctype.h>
#include <err.h>
#include <fcntl.h>
#include <glob.h>
#include <limits.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "libcfs/kp30.h"

#include "pfl/str.h"
#include "psc_util/alloc.h"
#include "psc_util/log.h"

#include "fid.h"
#include "slconfig.h"

enum slconf_sym_type {
	SL_TYPE_BOOL,
	SL_TYPE_FLOAT,
	SL_TYPE_HEXU64,
	SL_TYPE_INT,
	SL_TYPE_SIZET,
	SL_TYPE_STR,
	SL_TYPE_STRP
};

enum slconf_sym_struct {
	SL_STRUCT_VAR,
	SL_STRUCT_RES,
	SL_STRUCT_SITE
};

typedef uint32_t (*cfg_sym_handler_t)(const char *);

struct slconf_symbol {
	char			*c_name;
	enum slconf_sym_struct	 c_struct;
	enum slconf_sym_type	 c_type;
	uint64_t		 c_max;
	int			 c_offset;
	cfg_sym_handler_t	 c_handler;
};

struct cfg_file {
	char			 cf_fn[PATH_MAX];
	struct psclist_head	 cf_lentry;
};

void		 slcfg_add_include(const char *);
void		 slcfg_addif(char *, char *);
uint32_t	 slcfg_str2lnet(const char *);
uint32_t	 slcfg_str2restype(const char *);

void		 yyerror(const char *, ...);
int		 yylex(void);
int		 yyparse(void);
void		 yywarn(const char *, ...);

/*
 * Define a table macro for each structure type filled in by the config
 */
#define TABENT_VAR(name, type, max, field, handler)				\
	{ name, SL_STRUCT_VAR, type, max, offsetof(struct sl_gconf, field), handler }

#define TABENT_SITE(name, type, max, field, handler)				\
	{ name, SL_STRUCT_SITE, type, max, offsetof(struct sl_site, field), handler }

#define TABENT_RES(name, type, max, field, handler)				\
	{ name, SL_STRUCT_RES, type, max, offsetof(struct sl_resource, field), handler }

struct slconf_symbol sym_table[] = {
	TABENT_VAR("port",		SL_TYPE_INT,	0,		gconf_port,	NULL),
	TABENT_VAR("net",		SL_TYPE_STR,	LNET_NAME_MAX,	gconf_net,	NULL),
	TABENT_VAR("fs_root",		SL_TYPE_STR,	PATH_MAX,	gconf_fsroot,	NULL),
	TABENT_VAR("pref_ios",		SL_TYPE_STR,	RES_NAME_MAX,	gconf_prefios,	NULL),
	TABENT_VAR("pref_mds",		SL_TYPE_STR,	RES_NAME_MAX,	gconf_prefmds,	NULL),
	TABENT_SITE("site_id",		SL_TYPE_INT,	SITE_MAXID,	site_id,	NULL),
	TABENT_SITE("site_desc",	SL_TYPE_STRP,	0,		site_desc,	NULL),
	TABENT_RES ("desc",		SL_TYPE_STRP,	0,		res_desc,	NULL),
	TABENT_RES ("type",		SL_TYPE_INT,	0,		res_type,	slcfg_str2restype),
	TABENT_RES ("id",		SL_TYPE_INT,	RES_MAXID,	res_id,		NULL),
	TABENT_RES ("fsroot",		SL_TYPE_STR,	PATH_MAX,	res_fsroot,	NULL),
	TABENT_RES ("jrnldev",		SL_TYPE_STR,	PATH_MAX,	res_jrnldev,	NULL),
	{ NULL, 0, 0, 0, 0, NULL }
};

struct sl_gconf		 globalConfig;
struct sl_resm		*nodeResm;

int			 cfg_errors;
int			 cfg_lineno;
char			 cfg_filename[PATH_MAX];
struct psclist_head	 cfg_files = PSCLIST_HEAD_INIT(cfg_files);

struct sl_site		*currentSite;
struct sl_resource	*currentRes;
struct sl_gconf		*currentConf = &globalConfig;
%}

%start config

%token END
%token EQ

%token ATSIGN
%token NSEP

%token SUB
%token SUBSECT_START
%token SUBSECT_END

%token NUM
%token HEXNUM
%token NAME
%token PATHNAME
%token GLOBPATH
%token BOOL
%token SIZEVAL
%token FLOATVAL

%token SET
%token INCLUDE
%token RESOURCE_PROFILE
%token RESOURCE_NAME
%token RESOURCE_TYPE
%token SITE_PROFILE
%token SITE_NAME

%token IPADDR
%token PEERTAG
%token INTERFACETAG
%token QUOTEDS
%token LNETTCP

%%

config		: vars includes site_profiles
		;

vars		: /* NULL */
		| var vars
		;

var		: SET statement
		;

includes	: /* NULL */
		| include includes
		;

include		: INCLUDE QUOTEDS {
			glob_t gl;
			size_t i;
			int rc;

			rc = glob($2, GLOB_BRACE, NULL, &gl);
			if (rc)
				yywarn("%s: could not glob", $2);
			else {
				for (i = 0; i < (size_t)gl.gl_pathc; i++)
					slcfg_add_include($2);
				globfree(&gl);
			}
			PSCFREE($2);
		}
		;

site_profiles	: site_profile
		| site_profile site_profiles
		;

site_profile	: site_prof_start site_defs SUBSECT_END {
			if (libsl_siteid2site(currentSite->site_id))
				yyerror("site %s ID %d already assigned to %s",
				    currentSite->site_name, currentSite->site_id,
				    libsl_siteid2site(currentSite->site_id)->site_name);

			pll_add(&currentConf->gconf_sites, currentSite);
		}
		;

site_prof_start	: SITE_PROFILE SITE_NAME SUBSECT_START {
			struct sl_site *s;

			PLL_FOREACH(s, &globalConfig.gconf_sites)
				if (strcasecmp(s->site_name, $2) == 0)
					yyerror("duplicate site name: %s", $2);

			currentSite = PSCALLOC(sizeof(*currentSite));
			psc_dynarray_init(&currentSite->site_resources);
			INIT_PSC_LISTENTRY(&currentSite->site_lentry);
			if (strlcpy(currentSite->site_name, $2,
			    sizeof(currentSite->site_name)) >=
			    sizeof(currentSite->site_name))
				psc_fatalx("site %s: name too long", $2);
			slcfg_init_site(currentSite);
			PSCFREE($2);
		}
		;

site_defs	: statements site_resources
		;

site_resources	: site_resource
		| site_resources site_resource
		;

site_resource	: resource_start resource_def SUBSECT_END {
			struct sl_resource *r;
			int j, nmds = 0;

			if (strcmp(currentRes->res_name, "") == 0)
				yyerror("resource ID %d @%s has no name",
				    currentRes->res_id, currentSite->site_name);

			if (currentRes->res_type == SLREST_NONE)
				yyerror("resource %s@%s has no type specified",
				    currentRes->res_name, currentSite->site_name);

			currentRes->res_id = sl_global_id_build(
			    currentSite->site_id, currentRes->res_id);

			/* resource name & ID must be unique within a site */
			DYNARRAY_FOREACH(r, j, &currentSite->site_resources) {
				if (currentRes->res_id == r->res_id)
					yyerror("resource %s@%s ID "
					    "already assigned to %s",
					    currentRes->res_name,
					    currentRes->res_id,
					    r->res_name);
				if (strcasecmp(currentRes->res_name,
				    r->res_name) == 0)
					yyerror("duplicate resource name %s@%s",
					    currentRes->res_name,
					    currentSite->site_name);
			}

			psc_dynarray_add(&currentSite->site_resources, currentRes);

			DYNARRAY_FOREACH(r, j, &currentSite->site_resources)
				if (r->res_type == SLREST_MDS &&
				    ++nmds > 1)
					yyerror("site %s has more than "
					    "one metadata server",
					    currentSite->site_name);

			slcfg_init_res(currentRes);
		}
		;

resource_start	: RESOURCE_PROFILE NAME SUBSECT_START {
			struct sl_resource *r;
			int j, rc;

			currentRes = PSCALLOC(sizeof(*currentRes));
			currentRes->res_site = currentSite;
			psc_dynarray_init(&currentRes->res_peers);
			psc_dynarray_init(&currentRes->res_members);
			rc = snprintf(currentRes->res_name,
			    sizeof(currentRes->res_name), "%s%s",
			    $2, currentSite->site_name);
			if (rc == -1)
				psc_fatal("resource %s@%s",
				$2, currentSite->site_name);
			if (rc >= (int)sizeof(currentRes->res_name))
				psc_fatalx("resource %s@%s: name too long",
				    $2, currentSite->site_name);

			DYNARRAY_FOREACH(r, j, &currentSite->site_resources)
				if (strcasecmp(r->res_name,
				    currentRes->res_name) == 0)
					yyerror("duplicate resource name: %s",
					    r->res_name);

			PSCFREE($2);
		}
		;

resource_def	: statements
		;

peerlist	: PEERTAG EQ peers END
		;

peers		: peer
		| peer NSEP peers
		;

peer		: RESOURCE_NAME {
			if (currentRes->res_npeers >= SL_PEER_MAX)
				psc_fatalx("reached max (%d) npeers for resource",
				    SL_PEER_MAX);
			currentRes->res_peertmp[currentRes->res_npeers] = $1;
			currentRes->res_npeers++;
		}
		;

interfacelist	: INTERFACETAG EQ interfaces END
		;

interfaces	: interface
		| interface NSEP interfaces
		;

interface	: IPADDR ATSIGN LNETTCP	{ slcfg_addif($1, $3); PSCFREE($3); }
		| IPADDR		{ slcfg_addif($1, currentConf->gconf_net); }
		| NAME ATSIGN LNETTCP	{ slcfg_addif($1, $3); PSCFREE($3); }
		| NAME			{ slcfg_addif($1, currentConf->gconf_net); }
		;

statements	: /* NULL */
		| statement statements
		;

statement	: restype_stmt
		| path_stmt
		| num_stmt
		| bool_stmt
		| size_stmt
		| glob_stmt
		| hexnum_stmt
		| float_stmt
		| lnettcp_stmt
		| peerlist
		| interfacelist
		| quoteds_stmt
		;

restype_stmt	: NAME EQ RESOURCE_TYPE END {
			psclog_dbg("found restype statement: tok '%s' val '%s'",
			    $1, $3);
			slcfg_store_tok_val($1, $3);
			PSCFREE($1);
			PSCFREE($3);
		}
		;

path_stmt	: NAME EQ PATHNAME END {
			psclog_dbg("found path statement: tok '%s' val '%s'",
			    $1, $3);
			slcfg_store_tok_val($1, $3);
			PSCFREE($1);
			PSCFREE($3);
		}
		;

glob_stmt	: NAME EQ GLOBPATH END {
			psclog_dbg("found glob statement: tok '%s' Val '%s'",
			    $1, $3);
			slcfg_store_tok_val($1, $3);
			PSCFREE($1);
			PSCFREE($3);
		}
		;

bool_stmt	: NAME EQ BOOL END {
			psclog_dbg("found bool statement: tok '%s' val '%s'",
			    $1, $3);
			slcfg_store_tok_val($1, $3);
			PSCFREE($1);
			PSCFREE($3);
		}
		;

size_stmt	: NAME EQ SIZEVAL END {
			psclog_dbg("found sizeval statement: tok '%s' val '%s'",
			    $1, $3);
			slcfg_store_tok_val($1, $3);
			PSCFREE($1);
			PSCFREE($3);
		}
		;

num_stmt	: NAME EQ NUM END {
			psclog_dbg("found num statement: tok '%s' val '%s'",
			    $1, $3);
			slcfg_store_tok_val($1, $3);
			PSCFREE($1);
			PSCFREE($3);
		}
		;

float_stmt	: NAME EQ FLOATVAL END {
			psclog_dbg("found float statement: tok '%s' val '%s'",
			    $1, $3);
			slcfg_store_tok_val($1, $3);
			PSCFREE($1);
			PSCFREE($3);
		}
		;

hexnum_stmt	: NAME EQ HEXNUM END {
			psclog_dbg("found hexnum statement: tok '%s' val '%s'",
			    $1, $3);
			slcfg_store_tok_val($1, $3);
			PSCFREE($1);
			PSCFREE($3);
		}
		;

quoteds_stmt	: NAME EQ QUOTEDS END {
			psclog_dbg("found quoted string statement: tok '%s' val '%s'",
			    $1, $3);
			slcfg_store_tok_val($1, $3);
			PSCFREE($1);
			PSCFREE($3);
			/* XXX: don't free, just copy the pointer */
		}
		;

lnettcp_stmt	: NAME EQ LNETTCP END {
			psclog_dbg("found lnettcp string statement: tok '%s' val '%s'",
			    $1, $3);
			slcfg_store_tok_val($1, $3);
			PSCFREE($1);
			PSCFREE($3);
		}
		;

%%

void
slcfg_addif(char *ifname, char *netname)
{
	char nidstr[PSCRPC_NIDSTR_SIZE];
	struct sl_resm *resm;
	int rc;

	if (strchr(ifname, '-')) {
		yyerror("invalid interface name: %s", ifname);
		return;
	}

	if (strcmp(netname, "") == 0)
		yyerror("no LNET network specified");
	rc = snprintf(nidstr, sizeof(nidstr), "%s@%s", ifname,
	    netname);
	if (rc == -1)
		psc_fatal("snprintf");
	if (rc >= (int)sizeof(nidstr))
		psc_fatalx("interface name too long: %s", ifname);
	PSCFREE(ifname);

	resm = PSCALLOC(sizeof(*resm));
	psc_hashent_init(&globalConfig.gconf_nid_hashtbl, resm);
	rc = snprintf(resm->resm_addrbuf, sizeof(resm->resm_addrbuf),
	    "%s:%s", currentRes->res_name, nidstr);
	if (rc == -1)
		psc_fatal("resource member %s:%s", currentRes->res_name, nidstr);
	if (rc >= (int)sizeof(resm->resm_addrbuf))
		psc_fatalx("resource member %s:%s: address too long",
		    currentRes->res_name, nidstr);

	resm->resm_nid = libcfs_str2nid(nidstr);
	resm->resm_res = currentRes;
	slcfg_init_resm(resm);

	psc_hashtbl_add_item(&globalConfig.gconf_nid_hashtbl, resm);

	psc_dynarray_add(&currentRes->res_members, resm);
}

uint32_t
slcfg_str2restype(const char *res_type)
{
	if (!strcmp(res_type, "parallel_fs"))
		return (SLREST_PARALLEL_FS);
	if (!strcmp(res_type, "standalone_fs"))
		return (SLREST_STANDALONE_FS);
	if (!strcmp(res_type, "archival_fs"))
		return (SLREST_ARCHIVAL_FS);
	if (!strcmp(res_type, "cluster_noshare_fs"))
		return (SLREST_CLUSTER_NOSHARE_FS);
	if (!strcmp(res_type, "compute"))
		return (SLREST_COMPUTE);
	if (!strcmp(res_type, "mds"))
		return (SLREST_MDS);
	psc_fatalx("%s: invalid resource type", res_type);
}

struct slconf_symbol *
slcfg_get_symbol(const char *name)
{
	struct slconf_symbol *e;

	psclog_debug("symbol lookup '%s'", name);

	for (e = sym_table; e->c_name; e++)
		if (e->c_name && !strcmp(e->c_name, name))
			return (e);
	yyerror("symbol '%s' was not found", name);
	return (NULL);
}

void
slcfg_store_tok_val(const char *tok, char *val)
{
	struct slconf_symbol *e;
	char *endp;
	void *ptr;

	psclog_debug("val %s tok %s", val, tok);

	e = slcfg_get_symbol(tok);
	if (e == NULL)
		return;

	psclog_dbg("sym entry %p, name %s, type %d",
	    e, e->c_name, e->c_type);

	/*
	 * Access the correct structure based on the
	 *  type stored in the symtab entry. The offset
	 *  of the symbol was obtained with offsetof().
	 */
	switch (e->c_struct) {
	case SL_STRUCT_VAR:
		ptr = e->c_offset + (char *)currentConf;
		break;
	case SL_STRUCT_SITE:
		ptr = e->c_offset + (char *)currentSite;
		break;
	case SL_STRUCT_RES:
		ptr = e->c_offset + (char *)currentRes;
		break;
	default:
		psc_fatalx("invalid structure type %d", e->c_struct);
	}

	psclog_debug("type %d ptr %p", e->c_struct, ptr);

	switch (e->c_type) {
	case SL_TYPE_STR:
		if (strlcpy(ptr, val, e->c_max) >= e->c_max)
			yyerror("field %s value too large", e->c_name);
		psclog_debug("SL_TYPE_STR tok '%s' set to '%s'",
		    e->c_name, (char *)ptr);
		break;

	case SL_TYPE_STRP:
		*(char **)ptr = psc_strdup(val);
		psclog_debug("SL_TYPE_STRP tok '%s' set to '%s' %p",
		    e->c_name, *(char **)ptr, ptr);
		break;

	case SL_TYPE_HEXU64:
		*(uint64_t *)ptr = strtoull(val, &endp, 16);
		if (endp == val || *endp != '\0')
			yyerror("invalid value");
		if (e->c_max && *(uint64_t *)ptr > e->c_max)
			yyerror("field %s value too large", e->c_name);
		psclog_debug("SL_TYPE_HEXU64 tok '%s' set to '%"PRIx64"'",
		    e->c_name, *(uint64_t *)ptr);
		break;

	case SL_TYPE_INT:
		if (e->c_handler)
			*(int *)ptr = e->c_handler(val);
		else {
			long l;

			l = strtol(val, &endp, 10);
			if (l >= INT_MAX || l <= INT_MIN ||
			    endp == val || *endp != '\0')
				yyerror("%s: invalid integer", val);
			*(int *)ptr = l;
		}
		if (e->c_max && *(int *)ptr > (int)e->c_max)
			yyerror("field %s value too large", e->c_name);
		psclog_debug("SL_TYPE_INT tok '%s' set to '%d'",
		    e->c_name, *(int *)ptr);
		break;

	case SL_TYPE_BOOL:
		if (!strcasecmp("yes", val) ||
		    !strcasecmp("on", val) ||
		    !strcmp("1", val))
			*(int *)ptr = 1;
		else
			*(int *)ptr = 0;
		psclog_debug("SL_TYPE_BOOL option '%s' %s", e->c_name,
		    *(int *)ptr ? "enabled" : "disabled");
		break;

	case SL_TYPE_FLOAT:
		*(float *)ptr = strtof(val, &endp);
		if (endp == val || *endp != '\0')
			yyerror("invalid value");
		psclog_debug("SL_TYPE_FLOAT tok '%s' %f",
		    e->c_name, *(float *)ptr);
		break;

	case SL_TYPE_SIZET: {
		uint64_t i = 1;
		char *c;
		int j;

		j = strlen(val);
		if (j == 0)
			yyerror("invalid value");
		c = &val[j-1];

		switch (tolower(*c)) {
		case 'b':
			i = 1;
			break;
		case 'k':
			i = 1024;
			break;
		case 'm':
			i = 1024*1024;
			break;
		case 'g':
			i = UINT64_C(1024)*1024*1024;
			break;
		case 't':
			i = UINT64_C(1024)*1024*1024*1024;
			break;
		default:
			yyerror("sizeval '%s' has invalid postfix", val);
		}
		psclog_debug("szval = %"PRIu64, i);

		*c = '\0';
		*(uint64_t *)ptr = i * strtoull(val, &endp, 10);

		if (endp == val || *endp != '\0')
			yyerror("invalid value");

		if (e->c_max && *(uint64_t *)ptr > e->c_max)
			yyerror("field %s value too large", e->c_name);

		psclog_debug("SL_TYPE_SIZET tok '%s' set to '%"PRIu64"'",
			e->c_name, *(uint64_t *)ptr);
		break;
	    }

	default:
		yyerror("invalid token '%s' type %d", e->c_name, e->c_type);
	}
}

void
slcfg_add_include(const char *fn)
{
	struct cfg_file *cf;

	cf = PSCALLOC(sizeof(*cf));
	INIT_PSC_LISTENTRY(&cf->cf_lentry);
	if (strlcpy(cf->cf_fn, fn,
	    sizeof(cf->cf_fn)) >= sizeof(cf->cf_fn)) {
		errno = ENAMETOOLONG;
		psc_fatal("%s", fn);
	}
	psclist_add_tail(&cf->cf_lentry, &cfg_files);
}

void
slcfg_parse(const char *config_file)
{
	extern FILE *yyin;
	struct sl_resource *r, *peer;
	struct cfg_file *cf, *ncf;
	struct sl_site *s;
	int i, j;

	cfg_errors = 0;

	INIT_GCONF(&globalConfig);

	slcfg_add_include(config_file);
	psclist_for_each_entry_safe(cf, ncf, &cfg_files, cf_lentry) {
		if (realpath(cf->cf_fn, cfg_filename) == NULL)
			strlcpy(cfg_filename, cf->cf_fn,
			    sizeof(cfg_filename));
		yyin = fopen(cf->cf_fn, "r");
		if (yyin == NULL) {
			cfg_lineno = -1;
			yywarn("%s", strerror(errno));
		} else {
			cfg_lineno = 1;
			yyparse();
			fclose(yyin);
		}

		PSCFREE(cf);
	}
	if (cfg_errors)
		errx(1, "%d error(s) encountered", cfg_errors);

	PLL_LOCK(&globalConfig.gconf_sites);
	pll_sort(&globalConfig.gconf_sites, qsort, slcfg_site_cmp);
	PLL_FOREACH(s, &globalConfig.gconf_sites) {
		psc_dynarray_sort(&s->site_resources, qsort,
		    slcfg_res_cmp);
		DYNARRAY_FOREACH(r, j, &s->site_resources) {
			psc_dynarray_sort(&r->res_members, qsort,
			    slcfg_resm_cmp);

			/* Resolve peer names. */
			for (i = 0; i < r->res_npeers; i++) {
				peer = libsl_str2res(r->res_peertmp[i]);
				if (!peer)
					errx(1, "Peer resource %s not "
					    "specified", r->res_peertmp[i]);
				PSCFREE(r->res_peertmp[i]);
				psc_dynarray_add(&r->res_peers, peer);
			}
		}
	}
	PLL_ULOCK(&globalConfig.gconf_sites);
}

void
yyerror(const char *fmt, ...)
{
	char buf[LINE_MAX];
	va_list ap;

	cfg_errors++;

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	warnx("%s:%d: %s", cfg_filename, cfg_lineno, buf);
}

void
yywarn(const char *fmt, ...)
{
	char buf[LINE_MAX];
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	if (cfg_lineno == -1)
		warnx("%s: %s", cfg_filename, buf);
	else
		warnx("%s:%d: %s", cfg_filename, cfg_lineno, buf);
}
