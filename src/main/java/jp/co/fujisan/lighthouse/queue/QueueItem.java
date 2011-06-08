package jp.co.fujisan.lighthouse.queue;

import java.util.concurrent.locks.Lock;

import jp.co.fujisan.lighthouse.queue.ThreadFreeReadWriteLock.ThreadFreeReadLock;
import jp.co.fujisan.lighthouse.queue.ThreadFreeReadWriteLock.ThreadFreeWriteLock;
import jp.co.fujisan.lighthouse.queue.exception.LockTimeoutException;

public class QueueItem {
	final public static int CMD_CANCEL = 0;
	final public static int CMD_GET = 1;
	final public static int CMD_SET = 2;
	final public static int CMD_DELETE = 3;

	final public static Integer DELETED_VALUE = 0;
	
	
	protected QueueItem next = null;
	protected String key = null;
	protected int command = CMD_CANCEL;
	protected Object value = null;
	protected ThreadFreeReadWriteLock lock_obj = null;
	
	private QueueItemCommitter committer = null;
	
	public QueueItem(String key, Object value){
		this.key = key;
		this.value = value;
		lock_obj = new ThreadFreeReadWriteLock(0,0,true);
	}
	public QueueItem(String key, Object value,long lock_timeout_ms){
		this.key = key;
		this.value = value;
		lock_obj = new ThreadFreeReadWriteLock(lock_timeout_ms,lock_timeout_ms,true);
	}
	public QueueItem(QueueItem prev,String key, Object value,long lock_timeout_ms){
		this.key = key;
		this.value = value;
		lock_obj = new ThreadFreeReadWriteLock(lock_timeout_ms,lock_timeout_ms,true);
		if(prev!=null){
			prev.next = this;
		}
	}
	public QueueItem(int command,String key){
		this.command = command;
		this.key = key;
		this.value = DELETED_VALUE;
		lock_obj = new ThreadFreeReadWriteLock(0,0,true);
	}
	public QueueItem(int command,String key,long lock_timeout_ms){
		this.command = command;
		this.key = key;
		this.value = DELETED_VALUE;
		lock_obj = new ThreadFreeReadWriteLock(lock_timeout_ms,lock_timeout_ms,true);
	}
	public QueueItem(int command,String key, Object value){
		this.command = command;
		this.key = key;
		this.value = value;
		lock_obj = new ThreadFreeReadWriteLock(0,0,true);
	}
	public QueueItem(int command,String key, Object value,long lock_timeout_ms){
		this.command = command;
		this.key = key;
		this.value = value;
		lock_obj = new ThreadFreeReadWriteLock(lock_timeout_ms,lock_timeout_ms,true);
	}
	public QueueItem(QueueItem prev,int command,String key, Object value,long lock_timeout_ms){
		this.command = command;
		this.key = key;
		this.value = value;
		lock_obj = new ThreadFreeReadWriteLock(lock_timeout_ms,lock_timeout_ms,true);
		if(prev!=null){
			prev.next = this;
		}
	}
	
	public int command(){
		return command;
	}

	public String key(){
		return key;
	}
	
	public int getCommand(){
		return command;
	}

	public String getKey(){
		return key;
	}
	
	public Object value(){
		return value;
	}
	
	public final String toString(){
		switch(command){
		case CMD_CANCEL:
			return "("+this.hashCode()+")[cmd:CANCEL ,key:"+key+",value:"+value+"]";
		case CMD_GET:
			return "("+this.hashCode()+")[cmd: GET ,key:"+key+",value:"+value+"]";
		case CMD_SET:
			return "("+this.hashCode()+")[cmd: SET ,key:"+key+",value:"+value+"]";
		case CMD_DELETE:
			return "("+this.hashCode()+")[cmd: DELETE  ,key:"+key+",value:"+value+"]";
		}
		return "("+this.hashCode()+")[cmd: UNKNOWN  ,key:"+key+",value:"+value+"]";
	}
	
	public Lock lock_read() throws LockTimeoutException{
		return lock_read(0);
	}
	public Lock lock_read(long timeout_ms) throws LockTimeoutException{
		//StringBuffer msg = new StringBuffer();
		//msg.append(Thread.currentThread().getId()+": ["+Thread.currentThread().getStackTrace()[4].getMethodName()+"("+Thread.currentThread().getStackTrace()[4].getLineNumber()+")-->"+Thread.currentThread().getStackTrace()[3].getMethodName()+"("+Thread.currentThread().getStackTrace()[3].getLineNumber()+")-->"+ Thread.currentThread().getStackTrace()[2].getMethodName()+"("+Thread.currentThread().getStackTrace()[2].getLineNumber()+")]");
		try{
			if(lock_obj!=null){
				ThreadFreeReadLock r_lock = lock_obj.readLock();
				if(r_lock==null){
					throw new LockTimeoutException();
				}
				r_lock.lock(timeout_ms);
				//msg.append("read Locked. ");
				return r_lock;
			}
			//msg.append("does not Locked. ");
			return null;
		}finally{
			//msg.append(this.toString());
			//LightHouse.sync_debug(msg.toString());
		}
	}
	
	public Lock lock_write() throws LockTimeoutException{
		return lock_write(0);
	}
	public Lock lock_write(long timeout_ms) throws LockTimeoutException{
		//StringBuffer msg = new StringBuffer();
		//msg.append(Thread.currentThread().getId()+": ["+Thread.currentThread().getStackTrace()[4].getMethodName()+"("+Thread.currentThread().getStackTrace()[4].getLineNumber()+")-->"+Thread.currentThread().getStackTrace()[3].getMethodName()+"("+Thread.currentThread().getStackTrace()[3].getLineNumber()+")-->"+ Thread.currentThread().getStackTrace()[2].getMethodName()+"("+Thread.currentThread().getStackTrace()[2].getLineNumber()+")]");
		try{
			if(lock_obj!=null){
				ThreadFreeWriteLock w_lock = lock_obj.writeLock();
				if(w_lock==null){
					throw new LockTimeoutException();
				}
				w_lock.lock(timeout_ms);
				//msg.append("write Locked. ");
				return w_lock;
			}
			//msg.append("does not Locked. ");
			return null;
		}finally{
			//msg.append(this.toString());
			//LightHouse.sync_debug(msg.toString());
		}
	}

	public void unlock(){
		//StringBuffer msg = new StringBuffer();
		//msg.append(Thread.currentThread().getId()+": ["+Thread.currentThread().getStackTrace()[4].getMethodName()+"("+Thread.currentThread().getStackTrace()[4].getLineNumber()+")-->"+Thread.currentThread().getStackTrace()[3].getMethodName()+"("+Thread.currentThread().getStackTrace()[3].getLineNumber()+")-->"+ Thread.currentThread().getStackTrace()[2].getMethodName()+"("+Thread.currentThread().getStackTrace()[2].getLineNumber()+")]");
		if(lock_obj!=null){
			//if(lock_obj.isWriteLocked()){
				//msg.append("write");
			//}else{
				//msg.append("read");
			//}
			lock_obj.unlock();
			//msg.append("Unlocked. ");
		}
		//msg.append(this.toString());
		//LightHouse.sync_debug(msg.toString());
	}

	public QueueItem next() {
		// TODO Auto-generated method stub
		return next;
	}
	
	protected void setCommiter(QueueItemCommitter committer){
		this.committer = committer;
	}
	
	public void commit() throws Exception{
		if(this.committer!=null){
			this.committer.commit(this);
		}
		this.committer = null;
	}

}
