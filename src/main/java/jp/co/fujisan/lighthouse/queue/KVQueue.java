package jp.co.fujisan.lighthouse.queue;

import java.util.List;

import jp.co.fujisan.lighthouse.queue.exception.AlreadyFinalizedException;
import jp.co.fujisan.lighthouse.queue.exception.EnqueueTimeoutException;
import jp.co.fujisan.lighthouse.queue.exception.LockTimeoutException;
import jp.co.fujisan.lighthouse.queue.exception.NoMoreCapacityException;

public interface KVQueue {
	
	public final static Integer DELETED_VALUE = 0;
	//public final static QueueItem CANCELED_ITEM = new QueueItem(QueueItem.CMD_CANCEL,null,null);
	
	/**
	 * キューの最初からキーを取り出し、KVエントリ（QueueItem）を取得する。
	 * キューからキーは削除されるが、KVエントリはキャッシュには残っている。
	 * @return
	 * @throws Exception 
	 * @return　QueueItem if this queue changed as a result of the call
	 */
	public QueueItem dequeue() throws Exception;
	
	/**
	 * QueueItemをキューの最後に追加する。
	 * キューの内部でコマンドの整合性を取っているので、
	 * @param item
	 * @throws Exception
	 * @return　QueueItem　
	 * @throws NoMoreCapacityException 
	 * @throws AlreadyFinalizedException 
	 * @throws InterruptedException 
	 * @throws LockTimeoutException 
	 */
	public QueueItem enqueue(QueueItem item)throws EnqueueTimeoutException, AlreadyFinalizedException, NoMoreCapacityException, InterruptedException, LockTimeoutException;
	
	/**
	 * キーに対応するバリューを取り出す。
	 * キューおよびKVエントリは削除されない。
	 * 主に非同期DELETEオペレーション時の、GETリクエストの参照先に使われる。
	 * @param key
	 * @return
	 */
	public Object get(String key)throws AlreadyFinalizedException;

	/**
	 * キーに対応するアイテムをキャッシュのみにPUTする。
	 * キューにキーが存在した場合、キューから削除される。
	 * @param key
	 * @return
	 * @throws NoMoreCapacityException 
	 * @throws Exception 
	 */
	public QueueItem put(String key,QueueItem item)throws EnqueueTimeoutException,AlreadyFinalizedException, NoMoreCapacityException, Exception;

	/**
	 * キーに対応するキューおよびKVエントリを削除する。
	 * @param key
	 * @return　QueueItem
	 */
	public QueueItem remove(String key)throws EnqueueTimeoutException,AlreadyFinalizedException;

	public List<String> getQueuedKeys()throws AlreadyFinalizedException;

	public void compact()throws AlreadyFinalizedException;
	
	public void clear(boolean force)throws AlreadyFinalizedException;
	
	public boolean isEmpty();
	
	/**
	 * キュー長（アイテム数）の制限
	 * @param limit
	 * @return
	 */
	public void setQueueLimit(int limit);
	public int getQueueLimit();

	/**
	 * キュー長（アイテム数）の制限時のエンキュー待ち時間(ms)<br/>
	 * キューサイズが制限された場合の、スレッド待ち時間<br/>
	 * 待ち時間を超えた場合、EnqueueTimeoutExceptionがスローされる。
	 * @param limit
	 * @return
	 */
	public void setEnqueueWaitTime(long ms);
	public long getEnqueueWaitTime();

	public int queueSize();

	public int cacheSize();
	
	public boolean contains(String key)throws AlreadyFinalizedException;
	
	public void awake()throws InterruptedException;
	
	public void terminate()throws Exception;

	public void dump();
}
