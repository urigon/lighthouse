package jp.co.fujisan.lighthouse.queue;

import java.util.Iterator;

import jp.co.fujisan.lighthouse.queue.exception.AlreadyFinalizedException;
import jp.co.fujisan.lighthouse.queue.exception.EnqueueTimeoutException;
import jp.co.fujisan.lighthouse.queue.exception.LockTimeoutException;
import jp.co.fujisan.lighthouse.queue.exception.NoMoreCapacityException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 内部ストレージは、スレッドセーフなインタフェースを介してアクセスされるJavaの標準ライブラリを利用している。
 * 今のところこの実装が一番安全でパフォーマンスがよい。
 * 
 * @author development
 *
 */
public class KVQueueLockingImpl extends KVQueueBase implements QueueItemCommitter{
	
	protected Log logger = LogFactory.getLog(KVQueueLockingImpl.class);
	
	public KVQueueLockingImpl(){
		super();
	}
	public KVQueueLockingImpl(int lock_timeout_ms){
		super(lock_timeout_ms);
		this.lock_timeout_ms = lock_timeout_ms;
	}

	@Override
	public QueueItem enqueue(QueueItem item) throws AlreadyFinalizedException, InterruptedException, EnqueueTimeoutException, LockTimeoutException, NoMoreCapacityException{
		if(!isAvailable) throw new AlreadyFinalizedException("KVQueueLockingImpl is unavailable");
		
		if(item==null){
			return null;
		}
		
		String key = item.key;
		QueueItem prev_item = null;
		try{
			switch(item.command){
			case QueueItem.CMD_CANCEL:
				
				//存在するエントリを削除　→　エンキューしない
				prev_item = this.remove(key);
				return prev_item;
				
			case QueueItem.CMD_GET:
				
				//読み込みロックで既存エントリを取得
				//prev_item = ((LockingConcurrentHashMap)m_cache).get(key,LockingConcurrentHashMap.LOCK_READ);
				prev_item = ((LockingConcurrentHashMap)m_cache).get(key);
				//存在するエントリを返す　→　エンキューしない
				return prev_item;
				
			case QueueItem.CMD_SET:
			case QueueItem.CMD_DELETE:

				/*
				 * サイズ制限によるキュー待ち
				 */
				waitOnCheckQueueLimit();

				/*
				 * インキューエントリはコミット処理中ではないため、無条件で新しいエントリに置き換える。
				 * デキュー済エントリ=他のスレッドにより同期コミット処理中
				 * は、WriteLockを試行してLockを取得できた（コミット処理終了）らそれを新しいエントリーで置きかえる。
				 */
				//書き込みロックで既存エントリを新規エントリに置き換え
				prev_item = ((LockingConcurrentHashMap)m_cache).put(key,item,LockingConcurrentHashMap.LOCK_WRITE);
				//TODO R/W Lockよりもこの時点で対象キーをpopしたスレッドをwaitさせる
				if(!m_queue.contains(key)){
					if(!m_queue.offer(key)){
						throw new NoMoreCapacityException();
					}
				}
				synchronized(this){
					notify();
				}
				return prev_item;
				
			default:
			}
			
		}finally{
			item.unlock();
			if(prev_item!=null){
				prev_item.unlock();
			}

		}
		return null;
	}
	
	@Override
	public QueueItem dequeue() throws Exception{
			try{
				if(!isAvailable) throw new AlreadyFinalizedException("KVQueueLockingImpl is unavailable");
				String key = m_queue.poll();
				if(key==null){
					while(key==null){
						synchronized(this){
							wait();
						}
						if(!isAvailable) throw new AlreadyFinalizedException("KVQueueLockingImpl is unavailable");
						key = m_queue.poll();
					}
				}
				
				QueueItem item = null;
				//try{
					//書き込みロックでエントリを取得,ロックしたままリターン
					item = ((LockingConcurrentHashMap)m_cache).get(key,LockingConcurrentHashMap.LOCK_WRITE);
				//}catch(LockTimeoutException e){
    			//	logger.warn("#dequeue lock timedout ("+lock_timeout_ms+"ms) for ["+key+"]");
				//	throw e;
				//}
				if(item!=null){
					item.setCommiter(this);
				}
				return item;
				
			}catch(InterruptedException e){
				logger.warn("LockingKVQueueImpl.dequeue("+Thread.currentThread().getId()+") is interrupted.");
			}finally{
				/*
				 * サイズ制限によるキュー待ちスレッドを再開
				 */
				notifyOnCheckQueueLimit();
			}
			return null;
	}
	
	@Override
	public QueueItem put(String key, QueueItem item)throws AlreadyFinalizedException, EnqueueTimeoutException {
		if(!isAvailable) throw new AlreadyFinalizedException("KVQueueLockingImpl is unavailable");
		if(key==null||item==null){
			return null;
		}

		try{
			/*
			 * サイズ制限によるキュー待ち
			 */
			waitOnCheckQueueLimit();
			
		}catch(InterruptedException e){
			logger.warn("KVQueueLockingImpl.put("+Thread.currentThread().getId()+") is interrupted.");
			return null;
		}
		
		QueueItem prev_item = null;
		try{
			//書き込みロックで既存エントリを新規エントリに置き換え
			prev_item = ((LockingConcurrentHashMap)m_cache).put(key,item,LockingConcurrentHashMap.LOCK_READ);
			return prev_item;
		}catch(LockTimeoutException e){
			logger.warn("#put lock timedout ("+lock_timeout_ms+"ms) for ["+key+"]");
		} catch (InterruptedException e) {
			logger.warn("#failed to put ("+lock_timeout_ms+"ms) for ["+key+"]");
		}finally{
			if(prev_item!=null){
				prev_item.unlock();
			}
		}
		return null;
	}
		
	@Override
	public QueueItem commit(QueueItem item)throws Exception{
		if(!isAvailable) throw new AlreadyFinalizedException("KVQueueLockingImpl is unavailable");

		String key = item.key;
		try{
			item.lock_write();
			if(m_cache.containsValue(item)){
				item = ((LockingConcurrentHashMap)m_cache).remove(key);
			}
			return item;
		}catch(LockTimeoutException e){
			logger.warn("#commit lock timedout ("+lock_timeout_ms+"ms) for ["+key+"]");
		}finally{
			if(item!=null){
				item.unlock();
			}
		}
		return null;
	}
	
	@Override
	public QueueItem remove(String key) throws AlreadyFinalizedException{
		if(!isAvailable) throw new AlreadyFinalizedException("KVQueueLockingImpl is unavailable");

		QueueItem item = null;
		try{
			//書き込みロックで取得
			item = ((LockingConcurrentHashMap)m_cache).get(key,LockingConcurrentHashMap.LOCK_WRITE);
			if(item!=null){
				//存在するエントリを削除
				((LockingConcurrentHashMap)m_cache).remove(key);
			}
			m_queue.remove(key);
			return item;
		}catch(LockTimeoutException e){
			logger.warn("#remove lock timedout ("+lock_timeout_ms+"ms) for ["+key+"]");
		}finally{
			if(item!=null){
				item.unlock();
			}
		}
		return null;
	}

	@Override
	public void clear(boolean force) throws AlreadyFinalizedException {
		if(m_cache!=null){
			Iterator<QueueItem> ite = m_cache.values().iterator();
			while(ite.hasNext()){
				QueueItem item = ite.next();
				if(!force){
					try {
						item.lock_write();
					} catch (LockTimeoutException e) {
						continue;
					}
				}
				item.lock_obj.clear(force);
			}
			m_cache.clear();
		}
		super.clear(force);
	}
	
	@Override
	public void terminate()throws Exception {
		synchronized(this){
			notifyAll();
		}
		if(!isAvailable) throw new AlreadyFinalizedException("KVQueueLockingImpl is unavailable");
		super.terminate();
	}
}
