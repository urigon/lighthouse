package jp.co.fujisan.lighthouse.client;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
//import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import jp.co.fujisan.lighthouse.Configurations;

import tokyotyrant.RDB;
import tokyotyrant.transcoder.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TyrantClient extends KVSClient {

	protected Log logger = LogFactory.getLog(TyrantClient.class);
	
	RDBConnectionPool rdb_conn_pool = null;

	private boolean isStore_primitives_as_string = Configurations.CONFIG_DEFAULT_STORE_PRIMITIVES_AS_STRING;
	private boolean isCheck_client_availability_strictly = Configurations.CONFIG_DEFAULT_CHECK_CLIENT_AVAILABILITY_STRICTLY;
	
	/**
	 * @param name
	 * @param id
	 * @param host
	 * @throws UnknownHostException 
	 */
	public TyrantClient(String name, Integer id, int weight,String host,int host_port, Map<String,Object> context) throws Exception {
		super(name,id,weight,host,host_port,context);
		super.className = TyrantClient.class.getSimpleName();
		
		logger = LogFactory.getLog(TyrantClient.class);
		
		try{
			isStore_primitives_as_string = (Boolean)context.get(Configurations.CONFIG_KEY_STORE_PRIMITIVES_AS_STRING);
		}catch(Exception ignore){}
		try{
			isCheck_client_availability_strictly = (Boolean)context.get(Configurations.CONFIG_KEY_CHECK_CLIENT_AVAILABILITY_STRICTLY);
		}catch(Exception ignore){}
		
		conn_pool.clear();
		rdb_conn_pool = new RDBConnectionPool(address);
		conn_pool = rdb_conn_pool;
		

	}

	@Override
	final public String getType() {
		// TODO Auto-generated method stub
		return ClientFactory.SERVER_TYPE_TYRANT;
	}

	@Override
	final public boolean delete(String key) throws Exception {
		RDB rdb = null; 
		try{
			rdb = rdb_conn_pool.getRDB(isCheck_client_availability_strictly);
			return rdb.out(key);
		}catch(Exception e){
			onFail(e);
			if(isAvailable){
				return delete(key);
			}
			throw e;
		}finally{
			rdb_conn_pool.releaseRDB(rdb);
		}
	}

	@Override
	final public Object get(String key) throws Exception {
		RDB rdb = null; 
		try{
			rdb = rdb_conn_pool.getRDB(isCheck_client_availability_strictly);
			return rdb.get(key);
		}catch(Exception e){
			onFail(e);
			if(isAvailable){
				return get(key);
			}
			throw e;
		}finally{
			rdb_conn_pool.releaseRDB(rdb);
		}
	}

	@Override
	final public Map get(String[] keys) throws Exception {
		RDB rdb = null; 
		try{
			rdb = rdb_conn_pool.getRDB(isCheck_client_availability_strictly);
			return rdb.mget(keys);
		}catch(Exception e){
			onFail(e);
			if(isAvailable){
				return get(keys);
			}
			throw e;
		}finally{
			rdb_conn_pool.releaseRDB(rdb);
		}
	}

	@Override
	final public Set<String> keys() throws Exception {
		Set<String> result = new HashSet<String>();
		RDB rdb = null; 
		try{
			rdb = rdb_conn_pool.getRDB(isCheck_client_availability_strictly);
			Object[] keys = rdb.fwmkeys("", -1);
			if(keys!=null){
				for(int i=0;i<keys.length;i++){
					if(keys[i]!=null){
						result.add(desanitizeKey(keys[i].toString()));
					}
				}
			}
		}catch(Exception e){
			if(!isAvailable)
			onFail(e);
			if(isAvailable){
				return keys();
			}
			throw e;
		}finally{
			rdb_conn_pool.releaseRDB(rdb);
		}
		return result;
	}

	@Override
	final public Set<String> keys(String prefix) throws Exception {
		Set<String> result = new HashSet<String>();
		RDB rdb = null; 
		try{
			rdb = rdb_conn_pool.getRDB(isCheck_client_availability_strictly);
			Object[] keys = rdb.fwmkeys(prefix, -1);
			if(keys!=null){
				for(int i=0;i<keys.length;i++){
					if(keys[i]!=null){
						result.add(desanitizeKey(keys[i].toString()));
					}
				}
			}
		}catch(Exception e){
			if(!isAvailable)
			onFail(e);
			if(isAvailable){
				return keys(prefix);
			}
			throw e;
		}finally{
			rdb_conn_pool.releaseRDB(rdb);
		}
		return result;
	}

	@Override
	final public boolean set(String key, Object value) throws Exception {
		RDB rdb = null; 
		try{
			rdb = rdb_conn_pool.getRDB(isCheck_client_availability_strictly);
			if(!isStore_primitives_as_string){
				return rdb.put(key, value,getTransCoder(value));
			}else{
				return rdb.put(key, value);
			}
		}catch(Exception e){
			if(!isAvailable)
			onFail(e);
			if(isAvailable){
				return set(key,value);
			}
			throw e;
		}finally{
			rdb_conn_pool.releaseRDB(rdb);
		}
	}
	
	@Override
	final public boolean clear() throws Exception {
		RDB rdb = null; 
		try{
			rdb = rdb_conn_pool.getRDB(isCheck_client_availability_strictly);
			return rdb.vanish();
		}catch(Exception e){
			if(!isAvailable)
			onFail(e);
			if(isAvailable){
				return clear();
			}
			throw e;
		}finally{
			rdb_conn_pool.releaseRDB(rdb);
		}
	}

	@Override
	final public int size() throws Exception {
		RDB rdb = rdb_conn_pool.getRDB(isCheck_client_availability_strictly);
		try{
			return (int) rdb.rnum();
		}catch(Exception e){
			if(!isAvailable)
			onFail(e);
			if(isAvailable){
				return size();
			}
			throw e;
		}finally{
			rdb_conn_pool.releaseRDB(rdb);
		}
	}

	
	private static Transcoder getTransCoder(Object value){
		
		if(value instanceof Byte){
		    return new ByteTranscoder();
		}else if(value instanceof Byte[]){
			 return new ByteArrayTranscoder();
		}else if(value instanceof Double){
			 return new DoubleTranscoder();
		}else if(value instanceof Float){
			 return new FloatTranscoder();
		}else if(value instanceof Integer){
			 return new IntegerTranscoder();
		}else if(value instanceof Long){
			 return new LongTranscoder();
		}else if(value instanceof String){
			 return new StringTranscoder();
		}else if(value instanceof Serializable){
			 return new SerializableTranscoder();
		}else{
			 //return new SerializingTranscoder();
			 return new StringTranscoder();
		}
		
	}

	class RDBConnectionPool extends KVSClient.ConnectionPool{

		public RDBConnectionPool(InetSocketAddress address) throws Exception{
			super(address);
			
		}
		
		public void init() throws Exception{

			m_queue = new ConcurrentLinkedQueue<Integer>();
			m_cache = new ConcurrentHashMap<Integer,Object>(KVSClient.POOL_MAX_SIZE);
			
			if(this.address!=null){
				for(int i=0;i<KVSClient.POOL_INIT_SIZE;i++){
					createRDB(isCheck_client_availability_strictly);
				}
			}
		}
		
		private synchronized void createRDB(boolean checkConnectionAvailability)throws Exception{
			
			RDB rdb = new RDB();
			if(isSanitize_keys){
				rdb.setKeyTranscoder(new StringTranscoder(sanitize_encoding));
			}
			if(isStore_primitives_as_string){
				rdb.setValueTranscoder(new StringTranscoder(sanitize_encoding));
			}
			rdb.open(address);
			if(checkConnectionAvailability){
				long size = rdb.size();
				if(logger.isDebugEnabled()){
					logger.debug("RDBConnectionPool.createRDB("+Thread.currentThread().getId()+") = "+size);
				}
			}
			Integer hash_code = rdb.hashCode();			
			if(m_queue.offer(hash_code)){
				m_cache.put(hash_code, rdb);
				notify();
			}
		}
		
		public RDB getRDB(boolean checkConnectionAvailability){
			try{
				if(m_queue==null)
					throw new Exception("RDBConnectionPool is already finalized.");
				Integer id = m_queue.poll();
				if(id==null){
					if(m_cache.size()<KVSClient.POOL_MAX_SIZE)
					{
						createRDB(checkConnectionAvailability);
					}else{
						synchronized(this){
							wait();
						}
					}
					return getRDB(checkConnectionAvailability);
				}
				return (RDB)m_cache.get(id);
				
			}catch(InterruptedException e){
				logger.warn("RDBConnectionPool.getRDB("+Thread.currentThread().getId()+") is interrupted.");
			}catch(Exception e){
				logger.error(e);
			}
			return null;

		}
		
		public synchronized void releaseRDB(RDB rdb){

			if(rdb==null||m_queue==null){
				return;
			}
			try{
				if(m_queue.offer(rdb.hashCode())){
					notify();
				}
			}catch(Exception e){
				remove(rdb.hashCode());
			}
		}
		
		public synchronized void remove(int id){
			if(m_cache!=null){
				try{
					RDB rdb = (RDB)m_cache.remove(id);						
					rdb.close();
					notify();
				}catch(Exception ignore){
					if(logger.isDebugEnabled()){
						logger.debug(ignore);
					}
				}
			}
		}
	}

}
