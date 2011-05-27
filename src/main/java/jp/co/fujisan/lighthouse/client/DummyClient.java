package jp.co.fujisan.lighthouse.client;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DummyClient extends LocalClient {

	private static final Log logger = LogFactory.getLog(DummyClient.class);
	
	private String type = "unknown";
	
	/**
	 * @param name
	 * @param id
	 * @param host
	 * @throws UnknownHostException 
	 */
	public DummyClient(String ring_id,String name, Integer id, String type,Map<String,Object> context) throws Exception {
		super(ring_id,name,id,0,context);
		super.className = DummyClient.class.getSimpleName();
		this.type = type;
		this.isAvailable = false;
	}

	@Override
	public String getType() {
		// TODO Auto-generated method stub
		return this.type;
	}

	@Override
	public boolean isAvailable(boolean b){
		return false;
	}

	@Override
	public boolean setAvailable(boolean available){
		return false;
	}

	@Override
	public String toString(){
		String msg = "";
		ClientException e = (ClientException)context.get("failure");
		if(e!=null){
			msg = "failure="+e.getMessage();
		}
		return super.toString() + " " + msg;
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

	@Override
	public boolean delete(String[] keys) throws Exception {
		throw new UnsupportedOperationException(className + " has no capability to delete(List<String> keys)");
	}

	@Override
	public Map<String,Object> get(String[] keys) throws Exception {
		throw new UnsupportedOperationException(className + " has no capability to get(List<String> keys");
	}

	@Override
	public Set<String> keys(String prefix) throws Exception {
		throw new UnsupportedOperationException(className + " has no capability to keys(String prefix)");
	}

	@Override
	public boolean set(Map<String, Object> entries) throws Exception {
		throw new UnsupportedOperationException(className + " has no capability to set(Map<String, Object> entries)");
	}
	
}
