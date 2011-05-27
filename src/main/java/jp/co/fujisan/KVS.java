package jp.co.fujisan;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface KVS {

	//public boolean delete()throws Exception;
	public boolean delete(String key)throws Exception;
    public boolean delete(String group,String key)throws Exception;
    public boolean delete(String group,String[] keys)throws Exception;
    public boolean deleteGroup(String group)throws Exception;
	
	public Object get(String key)throws Exception;
	public Object get(String group,String key)throws Exception;
	public Map<String,Object> mget(String[] keys)throws Exception;
	public Map<String,Object> getGroup(String group)throws Exception;
	public Map<String,Object> getGroup(String group,String key_prefix)throws Exception;
	public Map<String,Object> getGroup(String group,String[] keys)throws Exception;
	
	public Long set(String key,Object value) throws Exception;
	public Long set(String group,String key,Object value)throws Exception;
	public Long setGroup(String group,Map<String,Object> entries)throws Exception;
	
	public Set<String> keys(String keyPrefix)throws Exception;
	public Set<String> keys(String group,String key_prefix)throws Exception;
	public int count(String key_prefix) throws Exception;
	public int count(String group,String key_prefix)throws Exception;
	
	public void lock(String key)throws Exception;
	public void lock(String group,String key)throws Exception;
	public void lockGroup(String group)throws Exception;
	public void unlock(String key)throws Exception;
	public void unlock(String group,String key)throws Exception;
	public void unlockGroup(String group)throws Exception;
	
}
