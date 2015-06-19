---
layout: post
title: mount_slash and oprofiler
author: pauln
type: progress
---

<pre>
Every 1.0s: /usr/local/oprof/bin/opreport -l /home/pauln/Code/p...  Fri Feb 20 19:02:29 2009

CPU: Core 2, speed 1861.91 MHz (estimated)
Counted CPU_CLK_UNHALTED events (Clock cycles when not halted) with a unit mask of 0x00 (Unh
alted core cycles) count 100000
samples  %        symbol name
281883    8.5757  fidc_child_get
172012    5.2331  psc_log_getlevel
123185    3.7476  _tands
112082    3.4099  _tands
96002     2.9207  __fidc_lookup_fg
67061     2.0402  psc_realloc
61288     1.8646  pscthr_get_canfail
52057     1.5837  .plt
50217     1.5277  libcfs_nid2str2
48670     1.4807  pscrpc_queue_wait
48245     1.4678  slash2fuse_listener_loop
46889     1.4265  slash2fuse_lookup_helper
40108     1.2202  reqlock
37342     1.1361  lnet_lookup_cookie
36247     1.1027  _tands
31848     0.9689  list_add
30684     0.9335  pscrpc_check_reply
30113     0.9161  psc_send_rpc
29668     0.9026  lnet_match_md
29663     0.9024  atomic_inc
29124     0.8860  _tands
28722     0.8738  fidc_gettime
28017     0.8524  lib_get_event
26676     0.8116  __fidc_lookup_inode
23277     0.7082  lnet_try_match_md
23163     0.7047  __pscrpc_free_req
23156     0.7045  atomic_dec_and_test
23151     0.7043  __list_add
22680     0.6900  usocklnd_read_msg
22562     0.6864  lnet_post_send_locked
22118     0.6729  atomic_dec
22031     0.6702  lnet_finalize
21960     0.6681  atomic_inc
20240     0.6158  lnet_md_unlink
19983     0.6079  ureqlock
19933     0.6064  lnet_find_peer_locked
19773     0.6016  lnet_enq_event_locked
19549     0.5947  _tands
19457     0.5919  lnet_parse
19407     0.5904  ureqlock
19382     0.5897  lnet_initialise_handle
19275     0.5864  spinlock
19076     0.5803  lib_md_build
18869     0.5741  LNetPut
18828     0.5728  lnet_return_credits_locked
18411     0.5601  LNetEQPoll
18290     0.5564  __list_add
17946     0.5460  reply_in_callback
17885     0.5441  atomic_dec
17865     0.5435  LNetMEAttach
17779     0.5409  lnet_commit_md
17153     0.5218  pscrpc_master_callback
17100     0.5202  atomic_add
16660     0.5068  usocklnd_send_tx
16440     0.5002  list_add
16058     0.4885  validlock
15911     0.4841  lnet_nid2ni_locked
15805     0.4808  list_del
15609     0.4749  slash2fuse_stat
15307     0.4657  lnet_match_blocked_msg
15144     0.4607  psc_unpack_msg
15024     0.4571  list_del
14957     0.4550  lnet_complete_msg_locked
14392     0.4378  pscrpc_prep_req_pool
13803     0.4199  __list_add
13660     0.4156  libcfs_lnd2netstrfns
12730     0.3873  after_reply
12571     0.3824  __list_add
12385     0.3768  list_empty
12356     0.3759  lnet_send
12109     0.3684  fcmh_clean_check
11929     0.3629  validlock
11847     0.3604  list_del
11730     0.3569  lnet_ni_addref_locked
11679     0.3553  usocklnd_find_peer_locked
11524     0.3506  usocklnd_create_tx
11473     0.3490  _tands
</pre>
