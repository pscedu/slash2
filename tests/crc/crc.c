#include <stdio.h>
#include <string.h>
#include "psc_util/crc.h"

#include "inode.h"

void
main(void)
{
	psc_crc_t crc;
	char b[1048576];
	sl_blkh_t bmapod;

	memset(b, 0, 1048576);
	PSC_CRC_CALC(crc, b, 1048576);
	printf("NULL 1MB buf CRC is 0x%llx\n", crc);
	
	memset(&bmapod, 0, sizeof(sl_blkh_t));
	PSC_CRC_CALC(crc, &bmapod, sizeof(sl_blkh_t));
	printf("NULL sl_blkh_t CRC is 0x%llx\n", crc);
}
