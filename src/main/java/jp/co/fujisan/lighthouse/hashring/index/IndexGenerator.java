package jp.co.fujisan.lighthouse.hashring.index;

public interface IndexGenerator {
	
	/**
	 * コンテンツ向けインデックスを生成
	 * @param key
	 * @param replicas
	 * @return
	 * @throws Exception
	 */
	public long genIndex(String key)throws Exception;
	
	/**
	 * 重み付けに基づき分散したノード向けインデックスを生成
	 * @param key
	 * @param replicas
	 * @return
	 * @throws Exception
	 */
	public long[] genNodeIndex(String key,int replicas)throws Exception;
	
	public int getSlice();

}
