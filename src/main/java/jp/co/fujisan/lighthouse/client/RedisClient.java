package jp.co.fujisan.lighthouse.client;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisClient extends KVSClient {

	/**
	 * @param name
	 * @param id
	 * @param host
	 * @throws UnknownHostException 
	 */
	public RedisClient(String name, Integer id, int weight, String host,int host_port, Map<String,Object> context) throws Exception {
		super(name,id,weight,host,host_port,context);
		super.className = RedisClient.class.getSimpleName();
	}

	@Override
	public String getType() {
		// TODO Auto-generated method stub
		return ClientFactory.SERVER_TYPE_REDIS;
	}


	@Override
	public Object get(String key)throws Exception {
		throw new UnsupportedOperationException(className + " has no capability to get(String key)");
	}

	@Override
	public boolean set(String key, Object value)throws Exception {
		throw new UnsupportedOperationException(className + " has no capability to set(String key, Object value)");
	}

	@Override
	public boolean delete(String key) throws Exception {
		throw new UnsupportedOperationException(className + " has no capability to delete(String key)");
	}

	@Override
	public Set<String> keys() throws Exception {
		throw new UnsupportedOperationException(className + " has no capability to keys()");
	}

}
