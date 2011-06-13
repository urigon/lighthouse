package jp.co.fujisan.lighthouse.client;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DebugClient extends LocalClient {

	private static final Log logger = LogFactory.getLog(DebugClient.class);

	public volatile Map<String,Object> store = null;
	/**
	 * @param name
	 * @param id
	 * @param host
	 * @throws UnknownHostException 
	 */
	public DebugClient(String name, Integer id, int weight,Map<String,Object> context) throws Exception {
		super(name,id,weight,context);
		super.className = DebugClient.class.getSimpleName();
		store = new ConcurrentHashMap<String,Object>();
		try{
			if(name.equalsIgnoreCase("failure")){
				throw new Exception("Debug failure occures on construction.");
			}
		}catch(Exception e){
			this.onFail(e);
			throw e;
		}
	}

	@Override
	public Object get(String key)throws Exception {
		// TODO Auto-generated method stub
		//logger.debug("[DebugClient:"+this.name+"] GET<"+key+">");
		if(!this.isAvailable){
			return null;
		}
		return store.get(sanitizeKey(key));
	}

	@Override
	public boolean set(String key, Object value)throws Exception {
		// TODO Auto-generated method stub
		//logger.debug("[DebugClient:"+this.name+"] SET<"+key+":"+value.toString()+">");
		if(!this.isAvailable){
			return false;
		}
		store.put(sanitizeKey(key), value);
		return true;
	}
	
	@Override
	public boolean delete(String key) throws Exception {
		// TODO Auto-generated method stub
		if(!this.isAvailable){
			return false;
		}
		if(store.remove(sanitizeKey(key))==null){
			return false;
		}
		return true;
	}

	@Override
	public String getType() {
		// TODO Auto-generated method stub
		return ClientFactory.SERVER_TYPE_DEBUG;
	}

	@Override
	public Set<String> keys() throws Exception {
		return store.keySet();
	}
	
	@Override
	public boolean setAvailable(boolean available){

		if(this.isAvailable==available){
			return this.isAvailable;
		}
		
		if(!available){
			store.clear();
		}
		return super.setAvailable(available);
	}

	@Override
	public void onFail(Exception e){
		store.clear();
		store = null;
		super.onFail(e);
	}

	@Override
	public void terminate() {
		store.clear();
		store = null;
		super.terminate();
	}

	@Override
	public int size() {
		return store.size();
	}
}
