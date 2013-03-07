
Bmap MDS Creation Management and Updating
In normal read-mode, the bmap is retrieved to give the list of IOS replicas (client) and the crc table (ION). The bmap has its own crc (bmap_crc) which protects the crc table and replication table against corruption.
File Pointer Extended Past Filesize
In this case we must create a new bmap on-the-fly with the crc table containing SL_NULL_CRC (the crc of all nulls).
Passing Hole Information to the Client
Since the client does not have access to the crc table, the MDS may communicate hole information by first checking to see if the crc for a given chunk is == SL_NULL_CRC. If it is then the MDS will set a bit to '0'. These bits will be put to the client in the bmap_get reply and will enable the client to know which chunks of the bmap must be retrieved from the ION.
Bmap Updates
Bmaps must be updated at several points:
Receipt of a chunk crc update causes two writes: the store of the chunk crc into its appropriate slot and the recomputing and rewriting of the bmap_crc.
Replica management: upon successful replication of the bmap or when replicas become invalid because of an overwrite. This also causes two writes (rewriting of the bmap_crc).
