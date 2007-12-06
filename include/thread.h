#ifndef SLASH_THREAD_H
#define SLASH_THREAD_H 1

/* Must stay sync'd with ZTHRT_*. */
const char *slashThreadTypeNames[] = {
	"slash_ctlthr",
	"slash_iothr%d",
	"slash_mdsthr%d",
	"slash_tcplndthr%d"
};

#define threadTypeNames slashThreadTypeNames

#define SLASH_CTLTHR 0
#define SLASH_IOTHR  1
#define SLASH_MDSTHR 2
#define SLASH_LNDTHR 3

#define NZTHRT 4

#endif
