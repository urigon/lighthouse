package jp.co.fujisan.lighthouse.queue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import jp.co.fujisan.lighthouse.queue.exception.LockFailureException;
import jp.co.fujisan.lighthouse.queue.exception.LockTimeoutException;

public class ThreadFreeReadWriteLock extends ReentrantReadWriteLock
{
	long read_lock_timemout_millis = 0;
	long write_lock_timemout_millis = 0;
	private volatile ReentrantReadWriteLock master_lock = null;
	private volatile ThreadFreeReadLock read_lock = null;
	private volatile ThreadFreeWriteLock write_lock = null;
	private volatile boolean cleared = true;
	private volatile Set<Thread> readerOwners = null;
	
	public ThreadFreeReadWriteLock(boolean fare)
	{
		super(fare);
		this.master_lock = new ReentrantReadWriteLock(fare);
		this.read_lock = new ThreadFreeReadLock(this);
		this.write_lock = new ThreadFreeWriteLock(this);
		readerOwners = Collections.synchronizedSet(new HashSet<Thread>());
		cleared = false;
	}
	public ThreadFreeReadWriteLock(long read_lock_timemout_millis,long write_lock_timemout_millis,boolean fare)
	{
		super(fare);
		this.master_lock = new ReentrantReadWriteLock(fare);
		this.read_lock = new ThreadFreeReadLock(this);
		this.write_lock = new ThreadFreeWriteLock(this);
		this.read_lock_timemout_millis = read_lock_timemout_millis;
		this.write_lock_timemout_millis = write_lock_timemout_millis;
		readerOwners = Collections.synchronizedSet(new HashSet<Thread>());
		cleared = false;
	}
	@Override
	public ThreadFreeReadLock readLock()
	{
		return this.read_lock;
	}
	@Override
	public ThreadFreeWriteLock writeLock()
	{
		return this.write_lock;
	}
	public void unlock(){
		if(cleared){
			return;
		}
		try{
			this.write_lock.unlock();
		}catch(Exception ignore){
		}
		this.read_lock.unlock();
		synchronized(this){
			this.notifyAll();
		}
	}
	
	private Thread getWriteOwner(){
		return this.getOwner();
	}
	
	public synchronized boolean isCleared(){
		return cleared;
	}
	
	public synchronized void clear(boolean force){
		cleared = false;
		synchronized(master_lock){
			master_lock.notifyAll();
		}
		if(force){
			Iterator<Thread> ite = this.getQueuedThreads().iterator();
			while(ite.hasNext()){
				Thread thread = ite.next();
				try{
					synchronized(thread){
						thread.interrupt();
					}
				}catch(Exception e){
					
				}
			}
		}else{
			Iterator<Thread> ite = this.getQueuedWriterThreads().iterator();
			while(ite.hasNext()){
				Thread thread = ite.next();
				try{
					synchronized(thread){
						thread.notify();
					}
				}catch(Exception e){
					
				}
			}
			ite = this.getQueuedReaderThreads().iterator();
			while(ite.hasNext()){
				Thread thread = ite.next();
				try{
					synchronized(thread){
						thread.notify();
					}
				}catch(Exception e){
					
				}
			}
		}
	}

	public class  ThreadFreeReadLock extends ThreadFreeReadWriteLock.ReadLock
	{
		public ThreadFreeReadLock(ThreadFreeReadWriteLock readwritelock){
			super(readwritelock);
		}
		
		@Override
		public void lock() {
			this.lock(0);
		}
		
		public void lock(long timeout_ms) {
			if(cleared){
				return;
			}
			if(readerOwners.contains(Thread.currentThread())){
				return;
			}
			master_lock.readLock().lock();
			try {
				write_lock.unlock();
				this.lock_internal(timeout_ms);
			} catch (LockTimeoutException e) {
				e.printStackTrace();
			} catch (LockFailureException e) {
				e.printStackTrace();
			}finally{
				master_lock.readLock().unlock();
			}
		}

		private void lock_internal(long timeout_ms)throws LockTimeoutException,LockFailureException {
			if(readerOwners.contains(Thread.currentThread())){
				return;
			}
			if(timeout_ms>0 || read_lock_timemout_millis>0){
				try {
					if(timeout_ms>0){
						if(super.tryLock(timeout_ms, TimeUnit.MILLISECONDS)){
							readerOwners.add(Thread.currentThread());
						}
					}
					if(read_lock_timemout_millis>0){
						if(super.tryLock(read_lock_timemout_millis, TimeUnit.MILLISECONDS)){
							readerOwners.add(Thread.currentThread());
							return;
						}
					}
					throw new LockTimeoutException();
				} catch (InterruptedException e) {
					throw new LockFailureException();
				}
			}else{
				super.lock();
				readerOwners.add(Thread.currentThread());
				return;
			}
		}
		
		@Override
		public void unlock() {
			if(!cleared){
				if(readerOwners.remove(Thread.currentThread())){
					super.unlock();
				}
			}
		}
		
	}
	public class  ThreadFreeWriteLock extends ThreadFreeReadWriteLock.WriteLock
	{

		public ThreadFreeWriteLock(ThreadFreeReadWriteLock readwritelock){
			super(readwritelock);
		}
		
		@Override
		public void lock() {
			this.lock(0);
		}

		public void lock(long timeout_ms) {
			if(cleared){
				return;
			}
			if(Thread.currentThread().equals(getWriteOwner())){
				return; 
			}
			master_lock.writeLock().lock();
			try{
				read_lock.unlock();
				try {
					this.lock_internal(timeout_ms);
				} catch (LockTimeoutException e) {
					e.printStackTrace();
				} catch (LockFailureException e) {
					e.printStackTrace();
				}
			}finally{
				master_lock.writeLock().unlock();
			}
		}

		private void lock_internal(long timeout_ms)throws LockTimeoutException,LockFailureException {
			if(Thread.currentThread().equals(getWriteOwner())){
				return;
			}
			if(timeout_ms>0||write_lock_timemout_millis>0){
				try {
					if(timeout_ms>0){
						if(this.tryLock(timeout_ms, TimeUnit.MILLISECONDS)){
							return;
						}
					}
					if(write_lock_timemout_millis>0){
						if(this.tryLock(write_lock_timemout_millis, TimeUnit.MILLISECONDS)){
							return;
						}
					}
					throw new LockTimeoutException();
				} catch (InterruptedException e) {
					throw new LockFailureException();
				}
			}else{
				super.lock();
				return;
			}
		}
		
		
		@Override
		public void unlock() {
			if(!cleared){
				if(Thread.currentThread().equals(getWriteOwner())){
					super.unlock();
				}				
			}
		}
	}
}

