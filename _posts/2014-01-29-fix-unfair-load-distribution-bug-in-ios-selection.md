---
layout: post
title: Fix unfair load distribution bug in IOS selection
author: yanovich
type: progress
---

I noticed an interesting problem on the <a
href="http://www.psc.edu/DataSupercell">DSC</a> deployment the other
day.
Performance was varying wildly with single threaded I/O tests (<tt>rsync
-P</tt>).
I took a look at the leases the MDS was assigning to gain some insight:

<pre>
yanovich@illusion2$ slmctl -sbml | awk '{print $3, $5}' | sort | uniq -c | column -t
      1 io-system		flags
      6 &lt;any&gt;		R---TB-----
      5 sense2s2@PSCARCH	-W--TB-----
      9 sense2s4@PSCARCH	-W--TB-----
    182 sense2s5@PSCARCH	-W--TB-----
      5 sense2s6@PSCARCH	-W--TB-----
      7 sense2s7@PSCARCH	-W--TB-----
      5 sense3s0@PSCARCH	-W--TB-----
      6 sense3s1@PSCARCH	-W--TB-----
      6 sense3s2@PSCARCH	-W--TB-----
      6 sense3s3@PSCARCH	-W--TB-----
      5 sense3s4@PSCARCH	-W--TB-----
      5 sense3s5@PSCARCH	-W--TB-----
      6 sense3s6@PSCARCH	-W--TB-----
      6 sense4s0@PSCARCH	-W--TB-----
      6 sense4s1@PSCARCH	-W--TB-----
      5 sense4s2@PSCARCH	-W--TB-----
      5 sense4s3@PSCARCH	-W--TB-----
      5 sense4s4@PSCARCH	-W--TB-----
      5 sense4s5@PSCARCH	-W--TB-----
      6 sense4s6@PSCARCH	-W--TB-----
     11 sense5s0@PSCARCH	-W--TB-----
      5 sense5s1@PSCARCH	-W--TB-----
      5 sense5s2@PSCARCH	-W--TB-----
      6 sense5s3@PSCARCH	-W--TB-----
     11 sense5s5@PSCARCH	-W--TB-----
      5 sense5s6@PSCARCH	-W--TB-----
      7 sense6s0@PSCARCH	-W--TB-----
      6 sense6s1@PSCARCH	-W--TB-----
      6 sense6s2@PSCARCH	-W--TB-----
      6 sense6s3@PSCARCH	-W--TB-----
      6 sense6s4@PSCARCH	-W--TB-----
      6 sense6s5@PSCARCH	-W--TB-----
      6 sense6s6@PSCARCH	-W--TB-----
      6 sense6s7@PSCARCH	-W--TB-----
</pre>

This command counts the number of occurrences of leases issued to each
I/O system.
There was an obvious problem with preferred treatment to sense2s5.
Examining the code, I see that we copy the list of I/O systems starting
from a position <em>P</em> in the list.
When we reach the end of the list, we start over from the beginning up
to position <em>P</em>.
Then, we increment <em>P</em> next time in an approach to round-robin
selection of I/O systems:

```ruby
require 'redcarpet'
markdown = Redcarpet.new("Hello World!")
puts markdown.to_html
```

slashd/mds.c:

```c_cpp
__static void
slm_resm_roundrobin(struct sl_resource *r, struct psc_dynarray *a)
{
	struct resprof_mds_info *rpmi = res2rpmi(r);
	struct sl_resm *m;
	int i, idx;

	RPMI_LOCK(rpmi);
	idx = slm_get_rpmi_idx(r);
	RPMI_ULOCK(rpmi);

	for (i = 0; i < psc_dynarray_len(&r->res_members); i++, idx++) {
		if (idx >= psc_dynarray_len(&r->res_members))
			idx = 0;

		m = psc_dynarray_getpos(&r->res_members, idx);
		psc_dynarray_add_ifdne(a, m);
	}
}
```

```c
static __inline int
slm_get_rpmi_idx(struct sl_resource *res)
{
	struct resprof_mds_info *rpmi;
	int locked, n;

	rpmi = res2rpmi(res);
	locked = RPMI_RLOCK(rpmi);
	if (rpmi->rpmi_cnt >= psc_dynarray_len(&res->res_members))
		rpmi->rpmi_cnt = 0;
	n = rpmi->rpmi_cnt++;
	RPMI_URLOCK(rpmi, locked);
	return (n);
}
```

```
static __inline int
slm_get_rpmi_idx(struct sl_resource *res)
{
	struct resprof_mds_info *rpmi;
	int locked, n;

	rpmi = res2rpmi(res);
	locked = RPMI_RLOCK(rpmi);
	if (rpmi->rpmi_cnt >= psc_dynarray_len(&res->res_members))
		rpmi->rpmi_cnt = 0;
	n = rpmi->rpmi_cnt++;
	RPMI_URLOCK(rpmi, locked);
	return (n);
}
```

In theory, this should work, but any servers that are unavailable will
give an unfair advantage to the first server in the list after a run of
such unavailable servers, as this first server will get hammered <em>N +
1</em> times if there are <em>N</em> unavailable servers.

This solution was to add the list and shuffle it, resulting in a much
nicer load distribution.
