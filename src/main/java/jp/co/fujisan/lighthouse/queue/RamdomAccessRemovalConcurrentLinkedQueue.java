package jp.co.fujisan.lighthouse.queue;

//import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import jp.co.fujisan.lighthouse.queue.exception.NoMoreCapacityException;

public class RamdomAccessRemovalConcurrentLinkedQueue extends ConcurrentLinkedQueue<String> {
	
	private static final String DELETED = "";
	private volatile Map<String,String> queuedKeys = null;
	//private Set<String> queuedKeys = null;
	protected volatile transient ReentrantReadWriteLock m_lock = null; 

	public RamdomAccessRemovalConcurrentLinkedQueue(){
		super();
		queuedKeys = new ConcurrentHashMap<String,String>();
		m_lock = new ReentrantReadWriteLock(true);
	}
	
	@Override
	public void clear(){
		queuedKeys.clear();
		super.clear();
	}
	
	@Override
	public String poll(){
		m_lock.readLock().lock();
		try {
			while(true){
				String key = super.poll();
				if(key!=null){
					key = queuedKeys.remove(key);
					if(key==null||DELETED.equals(key)){
						continue;
					}
				}else{
					if(queuedKeys.size()>0){
						Iterator<String> ite = queuedKeys.keySet().iterator();
						while(ite.hasNext()){
							key = queuedKeys.remove(ite.next());
							if(key==null||DELETED.equals(key)){
								continue;
							}
							break;
						}
					}
				}
				return key;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			m_lock.readLock().unlock();
		}
		return null;
	}
	
	@Override
	public boolean offer(String key){
		m_lock.readLock().lock();
		try{
			super.offer(key);
			queuedKeys.put(key, key);
			return true;
		}catch(Exception e){
		}finally{
			m_lock.readLock().unlock();
		}
		return false;
	}
	
	@Override
	public boolean remove(Object key){
		m_lock.readLock().lock();
		try{
//			List<String> arg = new ArrayList<String>();
//			arg.add(key);
//			if(super.removeAll(arg)){
//				queuedKeys.remove(key);
//				return true;
//			}
			if(queuedKeys.put((String)key,DELETED)!=null){
				return true;
			}
		}catch(Exception e){
		}finally{
			m_lock.readLock().unlock();
		}
		return false;
	}
	
	@Override
	public boolean contains(Object key){
		String value = queuedKeys.get(key);
		if(value==null||DELETED.equals(value)){
			return false;
		}else{
			return true;
		}
	}
	
	public int fragmentsize(){
		return queuedKeys.size() - this.size();
	}
	
	public int compact(){
		m_lock.writeLock().lock();
		try{
			if(queuedKeys.isEmpty()){
				super.clear();
			}else{
				//Compacting ConcurrentHashMap
				try{
					Iterator<String> ite = queuedKeys.keySet().iterator();
					while(ite.hasNext()){
						String key = ite.next();
						String value = queuedKeys.get(key);
						if(value==null||value.trim().length()==0){
							queuedKeys.remove(key);
						}
					}
				}catch(Exception e){
					
				}
				//Compacting ConcurrentLinkedQueue
				try{
					List<String> arg = new LinkedList<String>();
					Iterator<String> ite = super.iterator();
					while(ite.hasNext()){
						String key = ite.next();
						if(!queuedKeys.containsKey(key)){
							arg.add(key);
						}
					}
					super.removeAll(arg);
				}catch(Exception e){
					
				}
			}
			return super.size();
		}finally{
			m_lock.writeLock().unlock();
		}
	}

}
