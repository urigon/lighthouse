package jp.co.fujisan.lighthouse.client;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
//import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import jp.co.fujisan.lighthouse.Configurations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public abstract class LocalClient implements Client {

	private static final Log logger = LogFactory.getLog(LocalClient.class);

	protected String name;
	private Integer id;
	private int weight = 1;
	private int clone_number = 1;
	protected Map<String,Object> context = null;
	protected boolean isAvailable = false;
	
	protected String className = Client.class.getSimpleName();
	
	protected boolean isSanitize_keys = Configurations.CONFIG_DEFAULT_SANITIZE_KEY;
	protected String sanitize_encoding = Configurations.CONFIG_DEFAULT_SANITIZE_ENCODING;
	
	public LocalClient(){
	}
	
	/**
	 * @param name
	 * @param id
	 * @param host
	 * @throws UnknownHostException 
	 */
	public LocalClient(String name, Integer id, int weight,Map<String,Object> context) throws Exception {
		this.name = name;
		this.id = id;
		this.weight = weight;
		this.context = context;
		try{
			this.isSanitize_keys = (Boolean)context.get(Configurations.CONFIG_KEY_SANITIZE_KEY);
		}catch(Exception ignore){
		}
		try{
			this.sanitize_encoding = (String)context.get(Configurations.CONFIG_KEY_SANITIZE_ENCODING);
		}catch(Exception ignore){
		}
		
		this.isAvailable = false;
	}

	@Override
	public Integer getId() {
		// TODO Auto-generated method stub
		return this.id;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return this.name;
	}

	@Override
	public void setId(Integer id) {
		// TODO Auto-generated method stub
		this.id = id;
		
	}

	@Override
	public void setName(String name) {
		// TODO Auto-generated method stub
		this.name = name;
		
	}
	
	@Override
	public String toString(){
		return "["+className+"]" +
				"type=" + this.getType() + " " +
				"name=" + this.name + " " +
				"id="+this.id + " ";
	}

	@Override
	public int getWeight(){
		return this.weight;
	}
	
	@Override
	public int getCloneNumber(){
		return this.clone_number;
	}
	
	@Override
	public void setCloneNumber(int clone_number){
		this.clone_number = clone_number;
	}
	
	@Override
	public boolean isAvailable(boolean isStrictly){
		return isAvailable;
	}

	@Override
	public boolean setAvailable(boolean available){

		if(this.isAvailable==available){
			return this.isAvailable;
		}

		this.isAvailable = available;
		if(listener!=null){
			listener.availableEvent(this,this.isAvailable);
		}
		return this.isAvailable;
	}

	@Override
	public void terminate() {
		this.isAvailable = false;
		if(listener!=null){
			listener.terminateEvent(this);
		}

	}
	
	protected void onFail(Exception e){
		this.isAvailable = false;
		/*
		 * 情報の収集
		 */
		//StringBuffer buff = new StringBuffer();
		//buff.append(this.toString());
		//buff.append("\r\n");
		//buff.append(e.getMessage());
		this.context.put("failure", new ClientException(e));
		
		if(listener!=null){
			listener.failEvent(this, e);
		}

	}
	
	@Override
	public String getAdded(){
		if(this.context!=null){
			return (String)this.context.get("added");
		}
		return null;
	}

	@Override
	public String getCreated(){
		if(this.context!=null){
			return (String)this.context.get("created");
		}
		return null;
	}
	@Override
	public ClientException getFailure(){
		if(this.context!=null){
			return (ClientException)this.context.get("failure");
		}
		return null;
	}
	@Override
	public String getProperty(String key){
		if(this.context!=null){
			return (String)this.context.get(key);
		}
		return null;
	}
	
	protected String sanitizeKey( String key ) throws UnsupportedEncodingException {
		return ( isSanitize_keys ) ? URLEncoder.encode( key, sanitize_encoding ) : key;
	}
	protected String desanitizeKey( String key ) throws UnsupportedEncodingException {
		return ( isSanitize_keys ) ? URLDecoder.decode( key, sanitize_encoding ) : key;
	}

	
	/**
	 * マルチキー指定のメソッドは、グループキー指定のときのみLighthouseから呼ばれる。
	 * 同一ノードにストアされていないため。
	 * 
	 * ネイティブプロトコルでサポートされている場合は、これらのメソッドはオーバーライドされるべき。
	 */
	@Override
	public boolean set(Map<String, Object> entries) throws Exception {
		Iterator<Entry<String,Object>> ite = entries.entrySet().iterator();
		while(ite.hasNext()){
			Entry<String,Object> entry = ite.next();
			if(!set(entry.getKey(),entry.getValue())){
				return false;
			}
		}
		return true;
	}
	
	@Override
	public Map<String,Object> get(String[] keys) throws Exception {
		Map<String,Object> result = new HashMap<String,Object>();
		for(int i=0;i<keys.length;i++){
			String key = keys[i];
			result.put(key,get(key));
		}
		return result;
	}

	@Override
	public boolean delete(String[] keys) throws Exception {
		for(int i=0;i<keys.length;i++){
			if(!delete(keys[i])){
				return false;
			}
		}
		return true;
	}
	
	@Override
	public Set<String> keys(String prefix) throws Exception {
		Set<String> result = new HashSet<String>();
		Set<String> keys = keys();
		Iterator<String> ite = keys.iterator();
		while(ite.hasNext()){
			String key = desanitizeKey(ite.next());
			if(key.startsWith(prefix)){
				result.add(key);
			}
		}
		return result;
	}

	@Override
	public boolean clear() throws Exception {
		boolean result = true;
		
		Set<String> keys = keys();
		Iterator<String> ite = keys.iterator();
		while(ite.hasNext()){
			String key = ite.next();
			result = delete(key);
		}
		return result;
	}
	
	@Override
	public int size() throws Exception {
		return keys().size();
	}

	protected ClientEventListener listener = null;
	@Override
	public void setClientEventListener(ClientEventListener listener){
		this.listener = listener;
	}
	

}
