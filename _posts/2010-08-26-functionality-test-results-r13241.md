---
layout: post
title: Functionality test results (r13241)
author: pauln
type: progress
---

<ul class='expand'>
<li>STWT_SZV - FAIL (hang in the bmpc reaper.  Run after Iozone and MTWT)</li>
<li>MTWT_SZV - SUCCESS</li>
<li>IOZONE - SUCCESS</li>
<li>CREATE+STAT - NOTRUN</li>
</ul>

<h5>Iozone completes!</h5>
<pre class='code'>
	131072   16384  151040  152250    63398    63130   70574  110137   69885   742318   401939  113827    87376   63103    62733
	262144      64  127957  131976    50374    49968   51286  109407   50925   658132    53109   109602   109616   49703    48757
	262144     128  128460  130409    63484    64353   67561  109540   66389   939217    67346   61564    62272   63887    64436
	262144     256  124699  123667    64261    64619   66418  109579   66304   890023    68356   52875    60140   64129    63939
	262144     512  127747  128208    64963    64782   67166  109366   66881   910048    68942   51256    60371   63676    64156
	262144    1024  129086  132626    64449    64465   65850  109590   66171   836729    67849   45656    62659   64524    64553
	262144    2048  134714  135328    63958    64081   67473  109536   65392   718438    68401   41272    45193   63625    63095
	262144    4096  125039  127863    64413    64623   68209  109585   67035  1104080    69146   41757    58955   63877    63899
	262144    8192  125004  128574    64351    63647   66848  109677   63678   882054    72393   44599    46715   64699    64725
	262144   16384  124248  125691    64046    63929   68465  109797   63929   725180   405494   53512    63159   64061    63793
	524288      64  118469   85031    23169    30484   10701    7866   19810   672532     8548   107962   106495   20325    26591
	524288     128  105908   89033    25575    35472   18851   13254   19944   876107    18114   35142    33814   21247    30385
	524288     256  117260   95669    30480    34230   22685   29827   23022   817172    35354   30332    46171   21616    30748
	524288     512  116184   72952    26786    35881   28867   87851   27261   871125    38115   28145    42941   20739    30704
	524288    1024  117427   85795    28558    36190   37673  104597   27181   774002    38999   26876    40118   21071    30782
	524288    2048  107134   93798    28449    35137   37236  107733   30773   890050    39438   30477    52565   21195    31767
	524288    4096  119097   98051    28793    34432   37358  106891   29528  1109057    39114   30421    51219   22030    33060
	524288    8192  107633   96480    26593    34921   37258  107955   30603   889855    40154   35802    55059   22491    33081
	524288   16384  116682  106040    28736    34376   38013  107656   31217   736744    40580   36463    54324   23274    34070

iozone test complete.</pre>