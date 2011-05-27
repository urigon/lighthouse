package jp.co.fujisan.lighthouse.queue;

import jp.co.fujisan.lighthouse.queue.exception.AlreadyFinalizedException;
import jp.co.fujisan.lighthouse.queue.exception.NoMoreCapacityException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KVQueueSimpleImpl extends KVQueueBase{
	
	protected Log logger = LogFactory.getLog(KVQueueSimpleImpl.class);
	
	protected volatile long total_enqueued = 0;
	protected volatile long total_dequeued = 0;
	
	public KVQueueSimpleImpl(){
		super();
		total_enqueued = 0;
		total_dequeued = 0;
	}
	
	@Override
	public synchronized QueueItem enqueue(QueueItem item) throws AlreadyFinalizedException, NoMoreCapacityException{
		if(m_queue==null) throw new AlreadyFinalizedException("SimpleKVQueueImpl is unavailable");

		if(item==null){
			return null;
		}
		if(!checkQueueAvailable()){
			throw new NoMoreCapacityException();
		}
		String key = item.key;
		try{
			QueueItem replaced = this.remove(key);
			m_cache.put(key, item);
			total_enqueued++;
			if(!m_queue.offer(key)){
				m_cache.remove(key);
				throw new NoMoreCapacityException();
			}
			return replaced;
		}finally{
			notify();
		}
	}

	@Override
	public synchronized QueueItem dequeue() throws AlreadyFinalizedException {
		if(m_queue==null) throw new AlreadyFinalizedException("SimpleKVQueueImpl is unavailable");
		try{
			String key = m_queue.poll();
			if(key==null){
				wait();
				return dequeue();
			}
			QueueItem item = m_cache.remove(key);
			total_dequeued++;			
			return item;
			
		}catch(InterruptedException e){
			logger.warn("SimpleKVQueueImpl.dequeue("+Thread.currentThread().getId()+") is interrupted.");
		}
		
		return null;
	}
	
	@Override
	public synchronized QueueItem put(String key, QueueItem item) throws Exception{
//		if(key==null||item==null){
//			return null;
//		}
//		if(!m_queue.contains(key)){
//			if(!checkQueueAvailable()){
//				throw new NoMoreCapacityException();
//			}
//			m_queue.offer(key);
//		}
//		item = m_cache.put(key,item);
//		return item; 
		throw new Exception("not implemented.");
	}
	
	@Override
	public synchronized QueueItem remove(String key){
		m_queue.remove(key);
		QueueItem item = m_cache.remove(key);
		return item;
	}
	
	@Override
	public synchronized int queueSize() {
		return super.queueSize();
	}
	public synchronized long getTotalEnqueueCount() {
		return total_enqueued;
	}
	public synchronized long getTotalDequeueCount() {
		return total_dequeued;
	}

	@Override
	public synchronized void clear(boolean force) throws AlreadyFinalizedException{
		super.clear(force);
		total_enqueued = 0;
		total_dequeued = 0;
	}
	@Override
	public synchronized int cacheSize(){
		return super.cacheSize();
	}
	
	@Override
	public synchronized boolean contains(String key)throws AlreadyFinalizedException{
		return super.contains(key);
	}

	@Override
	public synchronized boolean isEmpty(){
		return super.isEmpty();
	}

}
