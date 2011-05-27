package jp.co.fujisan.lighthouse.hashring.index;


public class SimpleIndexGenerator implements IndexGenerator {

	public SimpleIndexGenerator() {
	}
	
	public long genIndex(String key) throws Exception {
		long hash   = 0;
		char[] cArr = key.toCharArray();

		for ( int i = 0; i < cArr.length; ++i ) {
			hash = (hash * 33) + cArr[i];
		}

		return hash;
	}

	@Override
	public long[] genNodeIndex(String key,int replicas) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getSlice() {
		// TODO Auto-generated method stub
		return 0;
	}

}
