/* $Id$ */

#ifndef HAVE_SLASHCONFIG_INC
#define HAVE_SLASHCONFIG_INC

#define SITE_NAME_MAX 64
#define RES_NAME_MAX  64
#define FULL_NAME_MAX (SITE_NAME_MAX+RES_NAME_MAX+2)

#define DEVNAMEMAX 128

#include <sys/param.h>

#include "lnet/types.h"
#include "libcfs/kp30.h"
#include "psc_util/alloc.h"
#include "psc_util/log.h"
#include "psc_ds/list.h"
#include "psc_ds/hash.h"
#include "psc_types.h"
//#include "sb.h"
#include "inode.h"


#define MAX_PEERS  32
#define MAX_IFS    256
#define MAXNET     32
#define INTSTR_MAX 10
#define MAXNET     32
#define DESC_MAX   255
#define RTYPE_MAX  32

#define slashGetConfig run_yacc

enum res_type_t {
	parallel_fs        = 1<<0,
	archival_fs        = 1<<1,
	cluster_noshare_fs = 1<<2,
	compute            = 1<<3,
	res_any            = 1<<4
};

typedef struct resource_profile {
	char                res_name[RES_NAME_MAX]; 
	char              **res_desc;
	char              **res_peertmp;
	unsigned int        res_id;
	unsigned int        res_mds;	
	enum res_type_t     res_type;
	sl_ios_id_t        *res_peers;
	u32                 res_npeers;
	lnet_nid_t         *res_nids;
	u32                 res_nnids;
	struct psclist_head res_list;
} sl_resource_t;

#define INIT_RES(r) INIT_PSCLIST_HEAD(&(r)->res_list)

typedef struct site_profile {
	char                site_name[SITE_NAME_MAX];
	char              **site_desc;
	u32                 site_id;	
	struct psclist_head site_resources;
	struct psclist_head site_list;
} sl_site_t;

#define INIT_SITE(s)						\
		INIT_PSCLIST_HEAD(&(s)->site_list) ;		\
		INIT_PSCLIST_HEAD(&(s)->site_resources)

typedef struct node_info_handle {
	sl_resource_t      *node_res;
	sl_site_t          *node_site;
} sl_nodeh_t;

typedef struct global_config {
	char                gconf_net[MAXNET];
	u32                 gconf_netid;
	int                 gconf_port;
	int                 gconf_nsites;
	struct psclist_head gconf_sites;
	struct hash_table   gconf_nids_hash;
} sl_gconf_t;

#define GCONF_HASHTBL_SZ 63
#define INIT_GCONF(g)							\
		INIT_PSCLIST_HEAD(&(g)->gconf_sites);			\
		init_hash_table(&(g)->gconf_nids_hash,			\
				GCONF_HASHTBL_SZ, "gconf_htbl")

typedef struct resource_member {
	lnet_nid_t        resm_nid;
	sl_resource_t    *resm_res;
	struct hash_entry resm_hashe;
} sl_resm_t;

extern sl_nodeh_t nodeInfo;
extern sl_gconf_t globalConfig;

static inline void
libsl_nid_associate(lnet_nid_t nid, sl_resource_t *res)
{
	sl_resm_t *resm = PSCALLOC(sizeof(sl_resm_t));
	
	resm->resm_nid = nid;
	resm->resm_res = res;
	init_hash_entry(&resm->resm_hashe, &resm->resm_nid, resm);
	add_hash_entry(&globalConfig.gconf_nids_hash, &resm->resm_hashe);
}

static inline sl_ios_id_t 
libsl_node2id(sl_nodeh_t *n)
{
	return (sl_global_id_build(n->node_site->site_id, 
				   n->node_res->res_id,
				   n->node_res->res_mds));
}

static inline sl_site_t *
libsl_id2site(sl_ios_id_t id)
{
	u32 tmp=(id >> (SL_RES_BITS + SL_MDS_BITS));
	sl_site_t *s=NULL;
	
	psc_assert(tmp <= ((1 << SL_SITE_BITS))-1);

	psclist_for_each_entry(s, &globalConfig.gconf_sites, site_list)
		if (tmp == s->site_id)			
			break;

	return s;
}

static inline sl_resource_t *
libsl_id2res(sl_ios_id_t id)
{
	sl_site_t *s=NULL;
	
	if ((s = libsl_id2site(id)) == NULL)
		return NULL;
	else {
		sl_resource_t *r=NULL;
		sl_ios_id_t    rid = sl_glid_to_resid(id);
		
		psclist_for_each_entry(r, &s->site_resources, res_list)
			if (rid == r->res_id) 
				break;

		return r;
	}
}

static inline sl_ios_id_t
libsl_str2id(const char *res_name)
{
	const char    *p = res_name;
	sl_site_t     *s=NULL;
	sl_resource_t *r=NULL;

	while (*p != (char)'@') {
		psc_assert((((int)(p-res_name)) < FULL_NAME_MAX) &&
			   p != '\0');
		p++;
	}
	psclist_for_each_entry(s, &globalConfig.gconf_sites, site_list) {
		if (!strncmp(s->site_name, p, SITE_NAME_MAX)) {
			psclist_for_each_entry(r, &s->site_resources, 
					       res_list) {
				if (!strncmp(r->res_name, res_name, 
					     FULL_NAME_MAX))
					return r->res_id;
			}				
		}		
        }
        return IOS_ID_ANY;
}

static inline void
libsl_profile_dump(void)
{
	sl_nodeh_t *z = &nodeInfo;
	u32 i;

	psc_warnx("\nNode Info: Resource ;%s;\n\tdesc: %s "
	       "\n\t ID (global=%u, local=%u, mds=%u)"
	       "\n\t Type %d, Npeers %u, Nnids %u",
	       z->node_res->res_name,
	       *z->node_res->res_desc,	       
	       libsl_node2id(z),
	       z->node_res->res_id,
	       z->node_res->res_mds,	       
	       z->node_res->res_type,
	       z->node_res->res_npeers,
	       z->node_res->res_nnids);

	for (i=0; i < z->node_res->res_npeers; i++) {
		sl_resource_t *r;
		
		r = libsl_id2res(z->node_res->res_peers[i]);
		if (!r)
			continue;
		psc_warnx("\n\t\tPeer %d ;%s;\t%s", 
		       i, r->res_name, *r->res_desc);		
	}
	for (i=0; i < z->node_res->res_nnids; i++) { 	
		psc_warnx("\n\t\tNid %d ;%s;", 
			  i, (char *)libcfs_nid2str(z->node_res->res_nids[i]));	
	}
}

static inline u32
libsl_str2restype(char *res_type)
{
	if (!strncmp(res_type, "parallel_fs", RES_NAME_MAX))
		return (parallel_fs);

	else if (!strncmp(res_type, "archival_fs", RES_NAME_MAX))
		return (archival_fs);

	else if (!strncmp(res_type, "cluster_noshare_fs", RES_NAME_MAX))
		return (cluster_noshare_fs);

	else if (strncmp(res_type, "compute", RES_NAME_MAX))
                return (compute);
	else 
		return (res_any);
}


int run_yacc(const char *config_file);

#endif /* HAVE_SLASHCONFIG_INC */
