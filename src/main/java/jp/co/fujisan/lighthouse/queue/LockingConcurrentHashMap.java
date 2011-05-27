package jp.co.fujisan.lighthouse.queue;

import java.util.concurrent.ConcurrentHashMap;

import jp.co.fujisan.lighthouse.queue.QueueItem;
import jp.co.fujisan.lighthouse.queue.exception.LockTimeoutException;

public class LockingConcurrentHashMap extends ConcurrentHashMap<String, QueueItem> {
	
	public static final int LOCK_READ = 1;
	public static final int LOCK_WRITE = 2;
	
	public LockingConcurrentHashMap(){
		super();
	}
	
	/**
	 * キーに紐づくエントリのQueueItemを指定のlock_modeでロックを取得してから返す。<br/>
	 * 取得されたエントリのQueueItemはロックを保持したまま戻り値として返される。<br/>
	 * 取得されたエントリのQueueItemのアンロックは呼び出し元によって行われないといけません。<br/>
	 * @param key
	 * @param lock_mode
	 * @return
	 * @throws LockTimeoutException 
	 */
	public QueueItem get(String key,int lock_mode) throws LockTimeoutException{
		QueueItem value = get(key);
		if(value!=null){
			switch(lock_mode){
			case LOCK_READ:
				value.lock_read();
				break;
			case LOCK_WRITE:
				value.lock_write();
				break;
			default:
			}
		}
		return value;	
	}
	
	/**
	 * 新規エントリのQueueItemを指定のlock_modeでロックを取得してからMapに挿入する。<br/>
	 * 既に同一キーでのエントリが存在している場合は、既存エントリのQueueItemを書き込みロックを取得してから置き換える。<br/>
	 * 置き換えられた古いエントリのQueueItemは書き込みロックを保持したまま戻り値として返される。<br/>
	 * 新規エントリのQueueItemは指定のlock_modeでロックを保持したままで関数は返ります。<br/>
	 * 新規エントリのQueueItemのアンロックは呼び出し元によって行われないといけません。<br/>
	 * @param key
	 * @param value
	 * @param lock_mode
	 * @return
	 * @throws LockTimeoutException 
	 * @throws LockTimeoutException
	 * @throws InterruptedException 
	 */
	public QueueItem put(String key,QueueItem value,int lock_mode) throws LockTimeoutException, InterruptedException{
		switch(lock_mode){
		case LOCK_READ:
			value.lock_read();
			break;
		case LOCK_WRITE:
			value.lock_write();
			break;
		default:
		}
		QueueItem prev = super.get(key);
		if(prev!=null){
			prev.lock_write();
		}
		QueueItem ret = super.put(key,value);
		if(prev!=null&&ret!=null&&prev.equals(ret)){
			return ret;
		}else{
			return this.put(key,value,lock_mode);
		}
	}
	
	/**
	 * キーに紐づくエントリロックを取得せずに削除する。<br/>
	 * 削除されたエントリのQueueItemは戻り値として返される。<br/>
	 * @param key
	 * @return
	 * @throws LockTimeoutException 
	 * @throws InterruptedException 
	 */
	public QueueItem remove(String key) throws LockTimeoutException{
//		QueueItem value = super.get(key);
//		if(value!=null){
//			value.lock_write();
//		}
		return super.remove(key);
	}
	
}
