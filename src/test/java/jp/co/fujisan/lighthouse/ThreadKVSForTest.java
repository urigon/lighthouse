package jp.co.fujisan.lighthouse;

import jp.co.fujisan.lighthouse.queue.KVQueueSimpleImpl;
import jp.co.fujisan.lighthouse.queue.QueueItem;
import jp.co.fujisan.lighthouse.queue.exception.AlreadyFinalizedException;
import jp.co.fujisan.lighthouse.queue.exception.NoMoreCapacityException;

public class ThreadKVSForTest extends KVQueueSimpleImpl{

	public ThreadKVSForTest(){
		super();
	}
	
	@Override
	public synchronized QueueItem enqueue(QueueItem item) throws AlreadyFinalizedException, NoMoreCapacityException{
		if(m_queue==null) throw new AlreadyFinalizedException("SimpleKVQueueImpl is unavailable");

		if(item==null){
			return null;
		}
		if(super.total_enqueued>=super.getQueueLimit()||!checkQueueAvailable()){
			throw new NoMoreCapacityException();
		}
		String key = item.key();
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


}
