package jp.co.fujisan.lighthouse.queue;

//import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import jp.co.fujisan.lighthouse.queue.exception.AlreadyFinalizedException;
import jp.co.fujisan.lighthouse.queue.exception.EnqueueTimeoutException;

/**
 * 内部ストレージは、スレッドセーフなインタフェースを介してアクセスされるJavaの標準ライブラリを利用している。
 * 今のところこの実装が一番安全でパフォーマンスがよい。
 * 
 * @author development
 *
 */
public abstract class KVQueueBase implements KVQueue{
	
	protected static Log logger = LogFactory.getLog(KVQueueBase.class);
	
	protected volatile boolean isAvailable = false;
	
	protected volatile int m_queue_limit = 0;
	protected volatile long m_enqueue_wait_ms = 0;
	protected volatile Queue<String> m_queue = null;
	protected volatile Map<String ,QueueItem> m_cache = null;
	protected int lock_timeout_ms = 0;
	
	private final static int QUEUE_FRAGMENT_THRESHOLD = 100;
	private static Timer queue_compressor =  null;
	protected static QueueCompressorTask queue_compressor_task = null;
	final static protected int COMPRESSION_INTERVAL = 30000; //30sec

	public KVQueueBase(){
		m_queue = new RamdomAccessRemovalConcurrentLinkedQueue();//ConcurrentLinkedQueue<String>();
		m_cache = new LockingConcurrentHashMap();
		init_compressor();
		isAvailable = true;
	}
	public KVQueueBase(int lock_timeout_ms){
		m_queue = new RamdomAccessRemovalConcurrentLinkedQueue();//ConcurrentLinkedQueue<String>();
		m_cache = new LockingConcurrentHashMap();
		this.lock_timeout_ms = lock_timeout_ms;
		init_compressor();
		isAvailable = true;
	}
	
	private void init_compressor(){
		if(queue_compressor==null){
			queue_compressor = new Timer("QueueCompressor");
			if(queue_compressor_task==null){
				queue_compressor_task = new QueueCompressorTask();
			}
			queue_compressor.schedule(queue_compressor_task,COMPRESSION_INTERVAL, COMPRESSION_INTERVAL);
		}
		if(!queue_compressor_task.contains((RamdomAccessRemovalConcurrentLinkedQueue)m_queue)){
			queue_compressor_task.addQueue((RamdomAccessRemovalConcurrentLinkedQueue)m_queue);
		}
	}
	
	private void cancel_compressor(){
		try{
			if(queue_compressor!=null){
				if(queue_compressor_task!=null){
					queue_compressor_task.removeQueue((RamdomAccessRemovalConcurrentLinkedQueue)m_queue);
					if(queue_compressor_task.size()==0){
						queue_compressor_task.cancel();
						queue_compressor_task = null;
						queue_compressor.cancel();
						queue_compressor.purge();
						queue_compressor = null;
					}
				}
			}
		}catch(Exception e){
			
		}
	}
	
	@Override
	public Object get(String key) throws AlreadyFinalizedException{
		if(!isAvailable) throw new AlreadyFinalizedException("KVQueue is unavailable");
		try{
			QueueItem item = m_cache.get(key);
			if(item!=null){
				return item.value();
			}
			return null; 
		}catch(Exception e){
			return null;
		}
	}
	
	@Override
	public synchronized void compact()throws AlreadyFinalizedException{
		if(!isAvailable) throw new AlreadyFinalizedException("KVQueue is unavailable");
		((RamdomAccessRemovalConcurrentLinkedQueue)m_queue).compact();
	}
	
	@Override
	public boolean isEmpty(){
		if(!isAvailable||m_queue==null||m_cache==null){
			return true;
		}
		return m_queue.isEmpty()&&m_cache.isEmpty();
	}

	@Override
	public List<String> getQueuedKeys()throws AlreadyFinalizedException {
		if(!isAvailable) throw new AlreadyFinalizedException("KVQueue is unavailable");
		Object[] keys = m_queue.toArray();
		List<String> ret =  new LinkedList<String>();
		for(int i=0;i<keys.length;i++){
			ret.add((String)keys[i]);
		}
		return ret;
	}

	@Override
	public void clear(boolean force)throws AlreadyFinalizedException {
		if(!isAvailable) throw new AlreadyFinalizedException("KVQueue is unavailable");
		if(m_queue!=null){
			m_queue.clear();
		}
		if(m_cache!=null){
			m_cache.clear();
		}
	}
	
	@Override
	public void terminate()throws Exception {
		if(!isAvailable) throw new AlreadyFinalizedException("KVQueue is unavailable");
		try {
			clear(true);
			cancel_compressor();
			isAvailable = false;
			awake();
		} catch (InterruptedException ignore) {
		}
	}
	
	@Override
	public void setQueueLimit(int limit){
		m_queue_limit = limit;
	}
	
	@Override
	public int getQueueLimit(){
		return m_queue_limit;
	}
	
	@Override
	public void setEnqueueWaitTime(long ms){
		if(ms<0){
			m_enqueue_wait_ms = 0;
		}else{
			m_enqueue_wait_ms = ms;
		}
	}
	
	@Override
	public long getEnqueueWaitTime(){
		return m_enqueue_wait_ms;
	}
	
	@Override
	public int queueSize() {
		if(!isAvailable||m_cache==null){
			return 0;
		}
		try {
			compact();
		} catch (AlreadyFinalizedException e) {
		}
		return m_queue.size();
	}

	@Override
	public int cacheSize(){
		if(!isAvailable||m_cache==null){
			return 0;
		}
		return m_cache.size();
	}
	
	@Override
	public boolean contains(String key)throws AlreadyFinalizedException{
		if(!isAvailable) throw new AlreadyFinalizedException("KVQueue is unavailable");
		return m_cache.containsKey(key);
	}
	
	@Override
	public void awake() throws InterruptedException {
		synchronized(this){
			notifyAll();
		}
	}
	
	public boolean checkQueueAvailable(){
		return 	isAvailable&&m_queue_limit>0 && m_queue!=null && m_queue_limit > m_queue.size() && m_cache!=null && m_queue_limit > m_cache.size();
	}
	
	/**
	 * サイズ制限によるキュー待ち
	 * @throws InterruptedException
	 * @throws EnqueueTimeoutException
	 */
	protected void waitOnCheckQueueLimit() throws InterruptedException, EnqueueTimeoutException{

		if(isAvailable&&m_queue_limit>0 && m_queue!=null && m_queue_limit <= m_queue.size() && m_cache!=null && m_queue_limit <= m_cache.size()){
			synchronized(this){
				while(true){
					long enter = System.currentTimeMillis();
					wait(m_enqueue_wait_ms);
					if(m_queue_limit > m_queue.size()){
						break;
					}
					if(m_queue_limit>0&&(System.currentTimeMillis()-enter) >= m_enqueue_wait_ms )
					{
						throw new EnqueueTimeoutException();
					}
				}
			}
		}
	}
	
	/**
	 * サイズ制限によるキュー待ち再開
	 */
	protected void notifyOnCheckQueueLimit(){
		if(isAvailable&&m_queue_limit>0 && m_queue!=null && m_queue_limit > m_queue.size() && m_cache!=null && m_queue_limit > m_cache.size()){
			synchronized(this){
				notify();
			}
		}
	}

	public synchronized void  dump(){
		try{
			try{
				//System.out.println("m_queue--------------------------");
				Iterator<String> ite = m_queue.iterator();
				while(ite.hasNext()){
					String key = ite.next();
					//System.out.println("<"+key+">");
				}
			}catch(Exception e){
				
			}
			try{
				//System.out.println("m_cache--------------------------");
				Iterator<String> ite = m_cache.keySet().iterator();
				while(ite.hasNext()){
					String key = ite.next();
					QueueItem item = m_cache.get(key);
					//System.out.println("<"+key+":"+item.toString()+">");
				}
			}catch(Exception e){
				
			}
		}finally{
			//System.out.println("---------------------------------");
		}
	}
	
	static class QueueCompressorTask extends TimerTask
	{
		private List<RamdomAccessRemovalConcurrentLinkedQueue> queues = null;
		public QueueCompressorTask(){
			queues = new LinkedList<RamdomAccessRemovalConcurrentLinkedQueue>();
		}

		@Override
		public void run() {
			synchronized(queues){
				Iterator<RamdomAccessRemovalConcurrentLinkedQueue> ite = queues.iterator();
				while(ite.hasNext()){
					
					RamdomAccessRemovalConcurrentLinkedQueue queue = ite.next();
					/*
					 * If current queue fragment size exceeds threshold size,
					 * Perform compaction.   
					 */
					int fragmentsize = queue.fragmentsize();
					if(fragmentsize>QUEUE_FRAGMENT_THRESHOLD){
						try{
							queue.compact();
							logger.debug("queue("+queue.hashCode()+") compressed ["+fragmentsize +"->"+ queue.fragmentsize()+"]");
						}catch(Exception e){
							logger.error("queue("+queue.hashCode()+") failed to compress ["+fragmentsize +"->"+ queue.fragmentsize()+"]",e);
						}
					}
				}
			}
		}

		public void addQueue(RamdomAccessRemovalConcurrentLinkedQueue queue){
			synchronized(queues){
				queues.add(queue);
			}
		}
			
		public void removeQueue(RamdomAccessRemovalConcurrentLinkedQueue queue){
			synchronized(queues){
				queues.remove(queue);
			}
		}

		public boolean contains(RamdomAccessRemovalConcurrentLinkedQueue queue){
			synchronized(queues){
				return queues.contains(queue);
			}
		}
		
		public int size(){
			if(queues==null){
				return 0;
			}
			return queues.size();
		}

		@Override
		public boolean cancel(){
			queues.clear();
			queues = null;
			return super.cancel();
		}
		
	}

}
