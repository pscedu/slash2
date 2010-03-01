/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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

#include "psc_util/alloc.h"
#include "psc_util/log.h"
#include "psc_util/strlcpy.h"

#include "slconfig.h"

enum sym_parameter_types {
	SL_TYPE_BOOL,
	SL_TYPE_FLOAT,
	SL_TYPE_HEXU64,
	SL_TYPE_INT,
	SL_TYPE_SIZET,
	SL_TYPE_STR,
	SL_TYPE_STRP
};

enum sym_structure_types {
	SL_STRUCT_GLOBAL,
	SL_STRUCT_RES,
	SL_STRUCT_SITE
};

typedef uint32_t (*sym_handler)(const char *);

struct symtable {
	char			*name;
	enum sym_structure_types sym_struct_type;
	enum sym_parameter_types sym_param_type;
	uint64_t		 max;
	int			 offset;
	sym_handler		 handler;
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

/*
 * Define a table macro for each structure type filled in by the config
 */
#define TABENT_GLBL(name, type, max, field, handler)				\
	{ name, SL_STRUCT_GLOBAL, type, max, offsetof(struct sl_gconf, field), handler }

#define TABENT_SITE(name, type, max, field, handler)				\
	{ name, SL_STRUCT_SITE, type, max, offsetof(struct sl_site, field), handler }

#define TABENT_RES(name, type, max, field, handler)				\
	{ name, SL_STRUCT_RES, type, max, offsetof(struct sl_resource, field), handler }

/* declare and initialize the global table */
struct symtable sym_table[] = {
	TABENT_GLBL("port",		SL_TYPE_INT,	0,		gconf_port,	NULL),
	TABENT_GLBL("net",		SL_TYPE_INT,	0,		gconf_netid,	slcfg_str2lnet),
	TABENT_SITE("site_id",		SL_TYPE_INT,	SITE_MAXID,	site_id,	NULL),
	TABENT_SITE("site_desc",	SL_TYPE_STRP,	0,		site_desc,	NULL),
	TABENT_RES ("desc",		SL_TYPE_STRP,	0,		res_desc,	NULL),
	TABENT_RES ("type",		SL_TYPE_INT,	0,		res_type,	slcfg_str2restype),
	TABENT_RES ("id",		SL_TYPE_INT,	RES_MAXID,	res_id,		NULL),
	TABENT_RES ("fsroot",		SL_TYPE_STR,	PATH_MAX,	res_fsroot,	NULL),
	{ NULL, 0, 0, 0, 0, NULL }
};

struct sl_gconf		 globalConfig;
struct sl_resm		*nodeResm;

int			 cfg_errors;
int			 cfg_lineno;
const char		*cfg_filename;
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

%token GLOBAL
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

config		: globals includes site_profiles
		;

globals		: /* NULL */
		| global globals
		;

global		: GLOBAL statement
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
				warnx("%s:%d: %s: could not glob",
				cfg_filename, cfg_lineno, $2);
			else {
				for (i = 0; i < gl.gl_pathc; i++)
					slcfg_add_include($2);
				globfree(&gl);
			}
			free($2);
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
			INIT_PSCLIST_ENTRY(&currentSite->site_lentry);
			if (strlcpy(currentSite->site_name, $2,
			    sizeof(currentSite->site_name)) >=
			    sizeof(currentSite->site_name))
				psc_fatalx("site %s: name too long", $2);
			slcfg_init_site(currentSite);
			free($2);
		}
		;

site_defs	: statements site_resources { }
		;

site_resources	: site_resource
		| site_resources site_resource { }
		;

site_resource	: resource_start resource_def SUBSECT_END {
			struct sl_resource *r;
			int j, nmds = 0;

			if (currentRes->res_type == SLREST_NONE)
				yyerror("resource %s has no type specified",
				    currentRes->res_name, currentRes->res_id);

			currentRes->res_id = sl_global_id_build(
			    currentSite->site_id, currentRes->res_id);

			if (libsl_id2res(currentRes->res_id))
				yyerror("resource %s ID %d already assigned to %s",
				    currentRes->res_name, currentRes->res_id,
				    libsl_id2res(currentRes->res_id)->res_name);

			psc_dynarray_add(&currentSite->site_resources, currentRes);

			DYNARRAY_FOREACH(r, j, &currentSite->site_resources)
				if (r->res_type == SLREST_MDS &&
				    ++nmds > 1)
					yyerror("more than one metadata server");
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

			slcfg_init_res(currentRes);
			free($2);
		}
		;

resource_def	: statements { }
		;

peerlist	: PEERTAG EQ peers END { }
		;

peers		: peer
		| peer NSEP peers { }
		;

peer		: RESOURCE_NAME {
			if (currentRes->res_npeers >= SL_PEER_MAX)
				psc_fatalx("reached max (%d) npeers for resource",
				    SL_PEER_MAX);
			currentRes->res_peertmp[currentRes->res_npeers] = $1;
			currentRes->res_npeers++;
		}
		;

interfacelist	: INTERFACETAG EQ interfaces END { }
		;

interfaces	: interface
		| interface NSEP interfaces { }
		;

interface	: IPADDR ATSIGN LNETTCP	{ slcfg_addif($1, $3); free($3); }
		| IPADDR		{ slcfg_addif($1, currentConf->gconf_net); }
		| NAME ATSIGN LNETTCP	{ slcfg_addif($1, $3); free($3); }
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
			psc_notify("Found Fstype Statement: Tok '%s' Val '%s'",
			   $1, $3);
			slcfg_store_tok_val($1, $3);
			free($1);
			free($3);
		}
		;

path_stmt	: NAME EQ PATHNAME END {
			psc_notify("Found Path Statement: Tok '%s' Val '%s'",
			       $1, $3);
			slcfg_store_tok_val($1, $3);
			free($1);
			free($3);
		}
		;

glob_stmt	: NAME EQ GLOBPATH END {
			psc_notify("Found Glob Statement: Tok '%s' Val '%s'",
			       $1, $3);
			slcfg_store_tok_val($1, $3);
			free($1);
			free($3);
		}
		;

bool_stmt	: NAME EQ BOOL END {
			psc_notify("Found Bool Statement: Tok '%s' Val '%s'",
			       $1, $3);
			slcfg_store_tok_val($1, $3);
			free($1);
			free($3);
		}
		;

size_stmt	: NAME EQ SIZEVAL END {
			psc_notify("Found Sizeval Statement: Tok '%s' Val '%s'",
			       $1, $3);
			slcfg_store_tok_val($1, $3);
			free($1);
			free($3);
		}
		;

num_stmt	: NAME EQ NUM END {
			psc_notify("Found Num Statement: Tok '%s' Val '%s'",
				$1, $3);
			slcfg_store_tok_val($1, $3);
			free($1);
			free($3);
		}
		;

float_stmt	: NAME EQ FLOATVAL END {
			psc_notify("Found Float Statement: Tok '%s' Val '%s'",
			       $1, $3);
			slcfg_store_tok_val($1, $3);
			free($1);
			free($3);
		}
		;

hexnum_stmt	: NAME EQ HEXNUM END {
			psc_notify("Found Hexnum Statement: Tok '%s' Val '%s'",
			       $1, $3);
			slcfg_store_tok_val($1, $3);
			free($1);
			free($3);
		}
		;

quoteds_stmt	: NAME EQ QUOTEDS END {
			psc_notify("Found Quoted String Statement: Tok '%s' Val '%s'",
				   $1, $3);
			slcfg_store_tok_val($1, $3);
			free($1);
			free($3);
			/* XXX: don't free, just copy the pointer */
		}
		;

lnettcp_stmt	: NAME EQ LNETTCP END {
			psc_notify("Found Lnettcp String Statement: Tok '%s' Val '%s'",
				   $1, $3);

			slcfg_store_tok_val($1, $3);
			free($1);
			free($3);
		}
		;

%%

void
slcfg_addif(char *ifname, char *netname)
{
	char nidstr[PSC_NIDSTR_SIZE];
	struct sl_resm *resm;
	int rc;

	if (strchr(ifname, '-')) {
		yyerror("invalid interface name: %s", ifname);
		return;
	}

	rc = snprintf(nidstr, sizeof(nidstr), "%s@%s", ifname, netname);
	if (rc == -1)
		psc_fatal("snprintf");
	if (rc >= (int)sizeof(nidstr))
		psc_fatalx("interface name too long: %s", ifname);
	free(ifname);

	resm = PSCALLOC(sizeof(*resm));
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
slcfg_str2lnet(const char *net)
{
	if (strlcpy(globalConfig.gconf_net, net,
	    sizeof(globalConfig.gconf_net)) >=
	    sizeof(globalConfig.gconf_net))
		psc_fatalx("LNET network name too long: %s", net);
	return (libcfs_str2net(net));
}

uint32_t
slcfg_str2restype(const char *res_type)
{
	if (!strcmp(res_type, "parallel_fs"))
		return (SLREST_PARALLEL_FS);
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

struct symtable *
slcfg_get_symbol(const char *name)
{
	struct symtable *e;

	psc_notify("symbol lookup '%s'", name);

	for (e = sym_table; e != NULL && e->name != NULL; e++)
		if (e->name && !strcmp(e->name, name))
			break;

	if (e == NULL || e->name == NULL) {
		psc_warnx("Symbol '%s' was not found", name);
		return NULL;
	}
	return (e);
}

void
slcfg_store_tok_val(const char *tok, char *val)
{
	struct symtable *e;
	void *ptr;

	psc_notify("val %s tok %s", val, tok);

	e = slcfg_get_symbol(tok);
	if (!e)
		psc_fatalx("%s: unknown symbol", tok);
	psc_trace("%p", e);

	psc_notify("sym entry %p, name %s, param_type %d",
		   e, e->name, e->sym_param_type);
	/*
	 * Access the correct structure based on the
	 *  type stored in the symtab entry.
	 */
	switch (e->sym_struct_type) {
	case SL_STRUCT_GLOBAL:
		ptr = e->offset + (char *)currentConf;
		break;
	case SL_STRUCT_SITE:
		ptr = e->offset + (char *)currentSite;
		break;
	case SL_STRUCT_RES:
		ptr = e->offset + (char *)currentRes;
		break;
	default:
		psc_fatalx("Invalid structure type %d", e->sym_struct_type);
	}

	psc_trace("Type %d ptr %p", e->sym_struct_type, ptr);

	switch (e->sym_param_type) {
	case SL_TYPE_STR:
		if (strlcpy(ptr, val, e->max) > e->max)
			yyerror("field %s value too large", e->name);
		psc_trace("SL_TYPE_STR Tok '%s' set to '%s'",
		       e->name, (char *)ptr);
		break;

	case SL_TYPE_STRP:
		*(char **)ptr = strdup(val);
		psc_trace("SL_TYPE_STRP Tok '%s' set to '%s' %p",
			  e->name, *(char **)ptr, ptr);
		break;

	case SL_TYPE_HEXU64:
		*(uint64_t *)ptr = strtoull(val, NULL, 16);
		if (e->max && *(uint64_t *)ptr > e->max)
			yyerror("field %s value too large", e->name);
		psc_trace("SL_TYPE_HEXU64 Tok '%s' set to '%"PRIx64"'",
		       e->name, *(uint64_t *)(ptr));
		break;

	case SL_TYPE_INT:
		if (e->handler)
			*(int *)ptr = (e->handler)(val);
		else {
			char *endp;
			long l;

			l = strtol(val, &endp, 10);
			if (l >= INT_MAX || l <= INT_MIN ||
			    endp == val || *endp != '\0')
				yyerror("%s: invalid integer", val);
			*(int *)ptr = l;
		}
		if (e->max && *(int *)ptr > (int)e->max)
			yyerror("field %s value too large", e->name);
		psc_trace("SL_TYPE_INT Tok '%s' set to '%d'",
		       e->name, *(int *)(ptr));
		break;

	case SL_TYPE_BOOL:
		*(int *)ptr = 0;
		if (!strcmp("yes", val) ||
		    !strcmp("1",   val)) {
			*(int *)ptr = 1;
			psc_trace("SL_TYPE_BOOL Option '%s' enabled", e->name);
		} else
			psc_trace("SL_TYPE_BOOL Option '%s' disabled", e->name);
		break;

	case SL_TYPE_FLOAT: {
		char   *c, floatbuf[17];
		float   f;

		f = atof(val);
		c = floatbuf;

		bzero(floatbuf, sizeof(floatbuf));
		snprintf(floatbuf, sizeof(floatbuf), "%f", f);

		psc_trace("float_val %f '%s'",
			f, floatbuf);

		while (*(c++) != '\0')
			if (*c == '.')
				*c = '\0';

		*(long *)ptr = strtol(floatbuf, NULL, 10);

		if (*c != '\0')
			*(long *)(ptr + 1) = strtol(c, NULL, 10);
		psc_trace("SL_TYPE_FLOAT Tok '%s' Secs %ld Usecs %lu",
			e->name, *(long *)ptr, *(long *)(ptr + 1));
		break;
	    }

	case SL_TYPE_SIZET: {
		uint64_t   i;
		int   j;
		char *c;

		j = strlen(val);
		psc_assert(j > 0);
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
			psc_fatalx("Sizeval '%c' is not valid", *c);
		}
		psc_trace("szval   = %"PRIu64, i);

		*c = '\0';
		*(uint64_t *)ptr = (i * strtoull(val, NULL, 10));

		if (e->max && *(uint64_t *)ptr > e->max)
			yyerror("field %s value too large", e->name);

		psc_trace("SL_TYPE_SIZET Tok '%s' set to '%"PRIu64"'",
			e->name, *(uint64_t *)ptr);
		break;
	    }

	default:
		psc_fatalx("invalid token '%s'", e->name);
	}
}

void
slcfg_add_include(const char *fn)
{
	struct cfg_file *cf;

	cf = PSCALLOC(sizeof(*cf));
	if (strlcpy(cf->cf_fn, fn,
	    sizeof(cf->cf_fn)) >= sizeof(cf->cf_fn)) {
		errno = ENAMETOOLONG;
		psc_fatal("%s", fn);
	}
	psclist_xadd_tail(&cf->cf_lentry, &cfg_files);
}

void
slcfg_parse(const char *config_file)
{
	extern FILE *yyin;
	struct sl_resource *r, *peer;
	struct cfg_file *cf, *ncf;
	struct sl_site *s;
	int n, j;

	cfg_errors = 0;

	INIT_GCONF(&globalConfig);

	slcfg_add_include(config_file);
	psclist_for_each_entry_safe(cf, ncf, &cfg_files, cf_lentry) {
		cfg_filename = cf->cf_fn;
		yyin = fopen(cfg_filename, "r");
		if (yyin == NULL)
			psc_fatal("%s", cfg_filename);

		cfg_lineno = 1;
		yyparse();
		fclose(yyin);

		free(cf);
	}
	if (cfg_errors)
		errx(1, "%d error(s) encountered", cfg_errors);

	PLL_LOCK(&globalConfig.gconf_sites);
	pll_sort(&globalConfig.gconf_sites, qsort, slcfg_site_cmp);
	PLL_FOREACH(s, &globalConfig.gconf_sites) {
		psc_dynarray_sort(&s->site_resources, qsort, slcfg_res_cmp);
		DYNARRAY_FOREACH(r, j, &s->site_resources) {
			psc_dynarray_sort(&r->res_members, qsort, slcfg_resm_cmp);

			/* Resolve peer names. */
			for (n = 0; n < r->res_npeers; n++) {
				peer = libsl_str2res(r->res_peertmp[n]);
				free(r->res_peertmp[n]);

				psc_assert(peer);
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
