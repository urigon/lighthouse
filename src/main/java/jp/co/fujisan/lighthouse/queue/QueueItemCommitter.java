package jp.co.fujisan.lighthouse.queue;


public interface QueueItemCommitter {
	public QueueItem commit(QueueItem item)throws Exception;
}
