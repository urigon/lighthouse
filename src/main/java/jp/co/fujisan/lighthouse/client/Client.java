package jp.co.fujisan.lighthouse.client;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Client {
	
	/**
	 * リングIDを返す
	 * @return
	 */
	public String getRingId();
		
	 /**
	 * @return the name
	 */
	public String getName();

	/**
	 * @param name the name to set
	 */
	public void setName(String name);

	/**
	 * @return the id
	 */
	public Integer getId();

	/**
	 * @param id the id to set
	 */
	public void setId(Integer id);

	public String getType();
	
	/**
	 * 設定ファイルからの重み付けプロパティ
	 * @return
	 */
	public int getWeight();

	/**
	 * HashRingに重み付けされて追加されたときのNodeのコピー数。
	 * @return
	 */
	public int getCloneNumber();

	public void setCloneNumber(int clones);
	
	public boolean isAvailable(boolean isStrictly);
	
	public boolean setAvailable(boolean available);
	
	public void terminate();
	
	public String getAdded();

	public String getCreated();

	public ClientException getFailure();
	
	public String getProperty(String key);

	/**
	 * Stores data on the server; only the key and the value are specified.
	 *クライアントはサーバー依存のエラーと内部エラーの切り分けをしなければならない.
	 * 
	 * Exception をthrowする場合：後続ノードへのSETオペレーションの試行を中止し速やかにエラーとする。
	 * 戻り値を返す場合：後続ノードへのSETオペレーションの試行を行う。
	 * 
	 * このインスタンスでリカバリー不能のエラーが発生した場合は、自らisActiveフラグをfalseとして、Fail outしRingから離脱する。
	 * Fail out 後は戻り値falseを返し、後続のノードにオペレーションを委譲することが望ましい。
	 *　
	 * @param key key to store data under
	 * @param value value to store
	 * @return true, if the data was successfully stored
	 */
	public boolean set(String key, Object value)throws Exception;
	public boolean set(Map<String,Object> entries)throws Exception;
	
	/**
	 * Retrieve a key from the server, using a specific hash.
	 *
	 *  If the data was compressed or serialized when compressed, it will automatically<br/>
	 *  be decompressed or serialized, as appropriate. (Inclusive or)<br/>
	 *<br/>
	 *  Non-serialized data will be returned as a string, so explicit conversion to<br/>
	 *  numeric types will be necessary, if desired<br/>
	 *  
	 * Exception をthrowする場合：後続ノードへのGETオペレーションの試行を中止し速やかにエラーとする。
	 * 戻り値を返す場合：後続ノードへのGETオペレーションの試行を行う。
	 * 
	 * このインスタンスでリカバリー不能のエラーが発生した場合は、自らisActiveフラグをfalseとして、Fail outしRingから離脱する。
	 * Fail out 後は戻り値falseを返し、後続のノードにオペレーションを委譲することが望ましい。
	 *
	 * @param key key where data is stored
	 * @return the object that was previously stored, or null if it was not previously stored
	 */
	public Object get(String key)throws Exception;
	public Map<String,Object> get(String[] keys)throws Exception;
	
	/**
	 * Delete a key from the server, using a specific hash.
	 *
	 *  If the data was compressed or serialized when compressed, it will automatically<br/>
	 *  be decompressed or serialized, as appropriate. (Inclusive or)<br/>
	 *<br/>
	 *  Non-serialized data will be returned as a string, so explicit conversion to<br/>
	 *  numeric types will be necessary, if desired<br/>
	 *  
	 * Exception をthrowする場合：後続ノードへのGETオペレーションの試行を中止し速やかにエラーとする。
	 * 戻り値を返す場合：後続ノードへのGETオペレーションの試行を行う。
	 * 
	 * このインスタンスでリカバリー不能のエラーが発生した場合は、自らisActiveフラグをfalseとして、Fail outしRingから離脱する。
	 * Fail out 後は戻り値falseを返し、後続のノードにオペレーションを委譲することが望ましい。
	 *
	 * @param key key where data is stored
	 * @return the object that was previously stored, or null if it was not previously stored
	 */
	public boolean delete(String key)throws Exception;
	public boolean delete(String[] keys)throws Exception;
	
	public Set<String> keys()throws Exception;
	public Set<String> keys(String prefix)throws Exception;

	public int size()throws Exception;
	
	public boolean clear()throws Exception;
	
	public void setClientEventListener(ClientEventListener listener);
}
