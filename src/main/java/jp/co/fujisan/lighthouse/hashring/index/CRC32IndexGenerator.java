package jp.co.fujisan.lighthouse.hashring.index;

import java.util.zip.CRC32;


public class CRC32IndexGenerator implements IndexGenerator {

	public final static int slice = 1;
	
	public CRC32IndexGenerator() {
	}
	
	@Override
	public long genIndex(String key) throws Exception {
		CRC32 checksum = new CRC32();
		checksum.update( key.getBytes() );
		long crc = checksum.getValue();
		return (crc >> 16) & 0x7fff;
	}

	@Override
	public long[] genNodeIndex(String key,int replicas) throws Exception {
		
		long[] ret = new long[replicas];
		
		for ( int i = 0; i < replicas; i++ ) {
				ret[i] = genIndex( key + "-" + i);
		}
		
		return ret;
	}

	public int getSlice() {
		// TODO Auto-generated method stub
		return slice;
	}

}
