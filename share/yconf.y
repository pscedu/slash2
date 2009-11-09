/* $Id$ */

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

enum sym_types {
	SL_FLAG,
	SL_FUNCTION,
	SL_METATAG,
	SL_VARIABLE
};

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
	enum sym_types		 sym_type;
	enum sym_structure_types sym_struct_type;
	enum sym_parameter_types sym_param_type;
	int			 param;
	int			 offset;
	sym_handler		 handler;
};

struct cfg_file {
	char			 cf_fn[PATH_MAX];
	struct psclist_head	 cf_lentry;
};

uint32_t	global_net_handler(const char *);
void		slcfg_addif(char *, char *);
void		slcfg_add_include(const char *);
void		yyerror(const char *, ...);
int		yyparse(void);
int		yylex(void);
void		store_tok_val(const char *, char *);

/*
 * Define a table macro for each structure type filled in by the config
 */
#define TABENT_GLBL(name, type, max, field, handler)				\
	{ name, SL_VARIABLE, SL_STRUCT_GLOBAL, type, max, offsetof(struct sl_gconf, field), handler }

#define TABENT_SITE(name, type, max, field, handler)				\
	{ name, SL_VARIABLE, SL_STRUCT_SITE, type, max, offsetof(struct sl_site, field), handler }

#define TABENT_RES(name, type, max, field, handler)				\
	{ name, SL_VARIABLE, SL_STRUCT_RES, type, max, offsetof(struct sl_resource, field), handler }

/* declare and initialize the global table */
struct symtable sym_table[] = {
	TABENT_GLBL("port",		SL_TYPE_INT,	0,		gconf_port,	NULL),
	TABENT_GLBL("net",		SL_TYPE_INT,	0,		gconf_netid,	global_net_handler),
	TABENT_GLBL("keyfn",		SL_TYPE_STR,	PATH_MAX,	gconf_fdbkeyfn,	NULL),
	TABENT_SITE("site_id",		SL_TYPE_INT,	0,		site_id,	NULL),
	TABENT_SITE("site_desc",	SL_TYPE_STRP,	0,		site_desc,	NULL),
	TABENT_RES ("desc",		SL_TYPE_STRP,	0,		res_desc,	NULL),
	TABENT_RES ("type",		SL_TYPE_INT,	0,		res_type,	libsl_str2restype),
	TABENT_RES ("id",		SL_TYPE_INT,	0,		res_id,		NULL),
	TABENT_RES ("mds",		SL_TYPE_BOOL,	0,		res_mds,	NULL),
	TABENT_RES ("fsroot",		SL_TYPE_STR,	PATH_MAX,	res_fsroot,	NULL),
	{ NULL, 0, 0, 0, 0, 0, NULL }
};

struct sl_gconf		 globalConfig;
struct sl_nodeh		 nodeInfo;

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

%token SEP
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

config         : globals includes site_profiles
{
	struct sl_resource *r;
	struct sl_site *s;
	uint32_t i;
	int n;

	/*
	 * Config has been loaded, iterate through the sites'
	 *  peer lists and resolve the names to numerical id's.
	 */
	PLL_FOREACH(s, &globalConfig.gconf_sites) {
		for (n = 0; n < s->site_nres; n++) {
			r = s->site_resv[n];

			r->res_peers = PSCALLOC(sizeof(sl_ios_id_t) *
						r->res_npeers);

			for (i=0; i < r->res_npeers; i++) {
				r->res_peers[i] = libsl_str2id(r->res_peertmp[i]);
				psc_assert(r->res_peers[i] != IOS_ID_ANY);
				free(r->res_peertmp[i]);
			}
			/*
			 * Associate nids with their respective resources,
			 *   and add the nids to the global hash table.
			 */
			for (i=0; i < r->res_nnids; i++)
				libsl_nid_associate(r->res_nids[i], r);
		}
	}
};

globals        : /* NULL */              |
		 global globals;

global         : GLOBAL statement;

includes	: /* NULL */		|
		  include includes;

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
		};

site_profiles  : site_profile            |
		 site_profile site_profiles;

site_profile   : site_profile_start site_defs SUBSECT_END
{
	pll_add(&currentConf->gconf_sites, currentSite);
	currentSite = slcfg_new_site();
};

site_profile_start : SITE_PROFILE SITE_NAME SUBSECT_START
{
	if (strlcpy(currentSite->site_name, $2,
	    SITE_NAME_MAX) >= SITE_NAME_MAX)
		psc_fatalx("site name too long");
	free($2);
};

site_defs      : statements site_resources
{};

site_resources : site_resource              |
		 site_resources site_resource { };

site_resource  : site_resource_start resource_def SUBSECT_END
{
	currentRes->res_id = sl_global_id_build(currentSite->site_id,
						currentRes->res_id);

	currentSite->site_resv = psc_realloc(currentSite->site_resv,
	    sizeof(*currentSite->site_resv) * (currentSite->site_nres + 1), 0);
	currentSite->site_resv[currentSite->site_nres++] = currentRes;
	currentRes->res_site = currentSite;

	/* setup next resource */
	currentRes = slcfg_new_res();
};

site_resource_start : RESOURCE_PROFILE NAME SUBSECT_START
{
	if (snprintf(currentRes->res_name, RES_NAME_MAX, "%s%s",
		     $2, currentSite->site_name) >= RES_NAME_MAX)
		psc_fatalx("Resource name too long");
	psc_trace("ResName %s", currentRes->res_name);
	free($2);
};

/*
resource_def   : statements interfacelist peerlist |
		 statements interfacelist          |
		 statements peerlist               |
		 statements interfacelist statements |
		 interfacelist statements          |
		 peerlist interfacelist statements |

{};
*/

resource_def : statements
{}

peerlist       : PEERTAG EQ peers END
{};

peers          : peer                              |
		 peer NSEP peers

{};

peer           : RESOURCE_NAME
{
	if (currentRes->res_npeers >= SL_PEER_MAX)
		psc_fatalx("reached max (%d) npeers for resource",
		    SL_PEER_MAX);
	currentRes->res_peertmp[currentRes->res_npeers] = $1;
	currentRes->res_npeers++;
};

interfacelist  : INTERFACETAG EQ interfaces END
{};

interfaces     : interface                 |
		 interface NSEP interfaces
{};

interface      : IPADDR ATSIGN LNETTCP	{ slcfg_addif($1, $3); free($3); }
	       | IPADDR			{ slcfg_addif($1, currentConf->gconf_net); }
	       | NAME ATSIGN LNETTCP	{ slcfg_addif($1, $3); free($3); }
	       | NAME			{ slcfg_addif($1, currentConf->gconf_net); }
	       ;

statements        : /* NULL */               |
		    statement statements;

statement         : restype_stmt |
		    path_stmt    |
		    num_stmt     |
		    bool_stmt    |
		    size_stmt    |
		    glob_stmt    |
		    hexnum_stmt  |
		    float_stmt   |
		    lnettcp_stmt |
		    peerlist     |
		    interfacelist|
		    quoteds_stmt;

restype_stmt : NAME EQ RESOURCE_TYPE END
{
	psc_notify("Found Fstype Statement: Tok '%s' Val '%s'",
		   $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
}

path_stmt : NAME EQ PATHNAME END
{
	psc_notify("Found Path Statement: Tok '%s' Val '%s'",
	       $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

glob_stmt : NAME EQ GLOBPATH END
{
	psc_notify("Found Glob Statement: Tok '%s' Val '%s'",
	       $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

bool_stmt : NAME EQ BOOL END
{
	psc_notify("Found Bool Statement: Tok '%s' Val '%s'",
	       $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

size_stmt : NAME EQ SIZEVAL END
{
	psc_notify("Found Sizeval Statement: Tok '%s' Val '%s'",
	       $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

num_stmt : NAME EQ NUM END
{
	psc_notify("Found Num Statement: Tok '%s' Val '%s'",
		$1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

float_stmt : NAME EQ FLOATVAL END
{
	psc_notify("Found Float Statement: Tok '%s' Val '%s'",
	       $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

hexnum_stmt : NAME EQ HEXNUM END
{
	psc_notify("Found Hexnum Statement: Tok '%s' Val '%s'",
	       $1, $3);
	store_tok_val($1, $3);
	free($1);
	free($3);
};

quoteds_stmt : NAME EQ QUOTEDS END
{
	psc_notify("Found Quoted String Statement: Tok '%s' Val '%s'",
		   $1, $3);

	store_tok_val($1, $3);
	free($1);
	free($3);
	/* XXX: don't free, just copy the pointer */
};

lnettcp_stmt : NAME EQ LNETTCP END
{
	psc_notify("Found Lnettcp String Statement: Tok '%s' Val '%s'",
		   $1, $3);

	store_tok_val($1, $3);
	free($1);
	free($3);
};

%%

void
slcfg_addif(char *ifname, char *netname)
{
	char nidstr[MAXNET];
	lnet_nid_t *i;
	int rc;

	if (strchr(ifname, '-')) {
		yyerror("invalid interface name: %s", ifname);
		return;
	}

	/* XXX dynarray */
	i = realloc(currentRes->res_nids,
	    (sizeof(lnet_nid_t) * (currentRes->res_nnids + 1)));
	psc_assert(i);

	rc = snprintf(nidstr, sizeof(nidstr), "%s@%s", ifname, netname);
	if (rc == -1)
		psc_fatal("snprintf");
	if (rc >= (int)sizeof(nidstr))
		psc_fatalx("interface name too long: %s", ifname);
	free(ifname);

	i[currentRes->res_nnids] = libcfs_str2nid(nidstr);

	psc_info("Got nidstr %s nid2str %s\n",
	    nidstr, libcfs_nid2str(i[currentRes->res_nnids]));

	currentRes->res_nnids++;
	currentRes->res_nids = i;
}

uint32_t
global_net_handler(const char *net)
{
	strlcpy(globalConfig.gconf_net, net, MAXNET);
	return (libcfs_str2net(net));
}

static struct symtable *
get_symbol(const char *name)
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
	return e;
}

void
store_tok_val(const char *tok, char *val)
{
	struct symtable *e;
	void            *ptr;

	psc_notify("val %s tok %s", val, tok);

	e = get_symbol(tok);
	if (!e)
		psc_fatalx("%s: unknown symbol", tok);
	psc_trace("%p %d", e, e->sym_type );
	psc_assert(e->sym_type == SL_VARIABLE ||
		   e->sym_type == SL_FLAG);

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
		strlcpy(ptr, val, e->param);
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
		psc_trace("SL_TYPE_INT Tok '%s' set to '%ld'",
		       e->name, (long)*(long *)(ptr));
		break;

	case SL_TYPE_BOOL:
		*(int *)ptr = 0;
		if ( !strncmp("yes", val, 3) ||
		     !strncmp("1",   val, 1) ) {
			//*(int *)ptr |= e->param;
			*(int *)ptr = 1;
			psc_trace("SL_TYPE_BOOL Option '%s' enabled", e->name);

		} else
			psc_trace("SL_TYPE_BOOL Option '%s' disabled", e->name);
		break;

	case SL_TYPE_FLOAT:
		{
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

			if ( *c != '\0' ) {
				*(long *)(ptr+1) = strtol(c, NULL, 10);
			}
			psc_trace("SL_TYPE_FLOAT Tok '%s' Secs %ld Usecs %lu",
				e->name, *(long *)ptr, *(long *)(ptr+1));
		}
		break;

	case SL_TYPE_SIZET: {
		uint64_t   i;
		int   j;
		char *c;

		j = strlen(val);
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
		psc_trace("ival   = %"PRIu64, i);

		*c = '\0';
		*(uint64_t *)ptr = (i * strtoull(val, NULL, 10));

		psc_trace("SL_TYPE_SIZET Tok '%s' set to '%"PRIu64"'",
			e->name, *(uint64_t *)ptr);
		break;
	    }

	default:
		psc_fatalx("Invalid Token '%s'", e->name);
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
	struct cfg_file *cf, *ncf;
	struct sl_resource *r;
	struct sl_site *s;
	int n;

	cfg_errors = 0;

	INIT_GCONF(&globalConfig);

	/* Pre-allocate the first resource and site */
	currentSite = slcfg_new_site();
	currentRes = slcfg_new_res();

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
		psc_fatalx("%d error(s) encountered", cfg_errors);

	free(currentRes);
	free(currentSite);

	pll_sort(&globalConfig.gconf_sites, qsort, slcfg_site_cmp);
	PLL_FOREACH(s, &globalConfig.gconf_sites) {
		qsort(s->site_resv, s->site_nres,
		    sizeof(*s->site_resv), slcfg_res_cmp);
		for (n = 0; n < s->site_nres; n++) {
			r = s->site_resv[n];
			qsort(r->res_nids, r->res_nnids,
			    sizeof(*r->res_nids), slcfg_resnid_cmp);
		}
	}
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

	psc_errorx("%s:%d: %s", cfg_filename, cfg_lineno, buf);
}
