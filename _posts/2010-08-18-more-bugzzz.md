---
layout: post
title: More bugzzz
author: pauln
type: progress
---

Read-before-writes being issued from the client needlessly:

<pre>
[1282083602:849976 sliricthr01:14541:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=98304 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=0
[1282083602:962496 sliricthr03:14543:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=229376 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083602:983695 sliricthr02:14542:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=360448 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083602:995708 sliricthr18:14558:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=491520 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083602:999686 sliricthr14:14554:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=622592 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:015023 sliricthr00:14540:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=753664 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:021103 sliricthr03:14543:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=884736 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:027943 sliricthr10:14550:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=1015808 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:035300 sliricthr09:14549:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=1146880 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:042009 sliricthr21:14561:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=1277952 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:047650 sliricthr08:14548:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=1409024 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:055499 sliricthr21:14561:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=1540096 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:061232 sliricthr14:14554:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=1671168 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:068275 sliricthr04:14544:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=1802240 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:074854 sliricthr08:14548:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=1933312 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:080775 sliricthr17:14557:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=2064384 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:088872 sliricthr21:14561:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=2195456 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:096729 sliricthr08:14548:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=2326528 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:103729 sliricthr21:14561:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=2457600 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083603:109248 sliricthr29:14569:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=2588672 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083606:738186 sliricthr19:14559:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=2719744 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083607:150602 sliricthr17:14557:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:2 sz=76546032 :: bmapno=1 size=32768 off=2850816 rw=42  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083607:185957 sliricthr08:14548:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:3 sz=76546032 :: bmapno=1 size=1048576 off=0 rw=43  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083607:204033 sliricthr21:14561:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:4 sz=76546032 :: bmapno=1 size=1048576 off=1048576 rw=43  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
[1282083607:221164 sliricthr20:14560:gen:sli_ric_handle_io:162] fcmh@0x71af20 fg:0x080000002d8eb0:0 DA ref:5 sz=76546032 :: bmapno=1 size=655344 off=2097152 rw=43  sbd_seq=4294967302 biod_cur_seqkey[0]=4294967302
</pre>
