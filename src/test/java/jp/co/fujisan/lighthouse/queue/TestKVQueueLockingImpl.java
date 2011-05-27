package jp.co.fujisan.lighthouse.queue;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import jp.co.fujisan.lighthouse.AppTest;
import jp.co.fujisan.lighthouse.ThreadKVSForTest;
import jp.co.fujisan.lighthouse.queue.KVQueue;
import jp.co.fujisan.lighthouse.queue.KVQueueSimpleImpl;
import jp.co.fujisan.lighthouse.queue.exception.AlreadyFinalizedException;
import junit.framework.TestCase;


public class TestKVQueueLockingImpl{
	
	private static final int iteration = AppTest.getLoopCount();
	private static boolean vervose = AppTest.isVervose();

	static int sampling_iteration = 100; 
	static String key_prefix = "KEY_PREFIX-";
	static String value_prefix = "VALUE_PREFIX-";
	private static ThreadKVSForTest kvs = null;
	private static ThreadFreeReadWriteLock locks = null;
	private static final Random random = new Random();

	static  int thread_multiply = 5; 
	static ExecutorService m_executor_service = null;
	static ScheduledExecutorService  m_executor_scheduled_service = null;
	static CountDownLatch enq_latch = null;
	static CountDownLatch deq_latch = null;
	static CountDownLatch out_latch = null;

	public TestKVQueueLockingImpl(){}
	@Before
	public void setUp() throws Exception {
    	m_executor_service = Executors.newCachedThreadPool();
    	m_executor_scheduled_service = Executors.newScheduledThreadPool(thread_multiply*2);
        kvs = new ThreadKVSForTest();
		kvs.setQueueLimit(iteration*2);
        locks = new ThreadFreeReadWriteLock(true);
    	enq_latch = new CountDownLatch(iteration*2);
    	deq_latch = new CountDownLatch(iteration);
    	out_latch = new CountDownLatch(iteration);
	}

	@After
	public void tearDown() throws Exception{
		m_executor_service.shutdown();
		m_executor_service = null;
		m_executor_scheduled_service.shutdown();
		m_executor_scheduled_service = null;
		AppTest.cancelLatch(enq_latch);
		AppTest.cancelLatch(deq_latch);
		AppTest.cancelLatch(out_latch);
		kvs.clear(false);
		locks.clear(true);
		System.out.println("====>Passed\r\n\r\n");
	}
	
	
	@Test
	public void testSingleThread_Benchmark(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
			KVQueue test = new KVQueueLockingImpl();
			simpleBenchMark(test);
	        assertTrue(true);
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}

	}
	
	private void simpleBenchMark(KVQueue test){

		double enq_time = 0;
		double ovr_time = 0;
		double deq_time = 0;
		double get_time = 0;
		double out_time = 0;
        
		int time = 0;
		while(time<sampling_iteration){
			if(AppTest.isVervose())
			System.out.println("-----------------------------------------");
			//Enqueue
    		int i =0;
    		try{
    			long start = System.currentTimeMillis();
                while(i<iteration){
                	String key = key_prefix + i;
                	//test.enqueue(new QueueItem(QueueItem.CMD_SET,key,value_prefix + i));
                	test.enqueue(new QueueItem(QueueItem.CMD_DELETE,key));
                	i++;
                }
                try{
            		long end = System.currentTimeMillis();
            		double taken = ((double)(end-start)/1000);
            		double item_perSec = ((double)iteration/taken);
            		enq_time +=  taken;
        			if(AppTest.isVervose())
            		System.out.println(" enqueue "+iteration+" times in "+taken+" sec ["+item_perSec+" operations pr.sec]");
                }catch(Exception e){
                	
                }
    		}catch(Exception e){
    			e.printStackTrace();
    			fail();
    		}finally{
                try {
					test.compact();
				} catch (AlreadyFinalizedException e) {
				}
    		}

    		//Enqueue override
    		i=0;
    		try{
    			long start = System.currentTimeMillis();
                while(i<iteration){
                	String key = key_prefix + i;
                	test.enqueue(new QueueItem(QueueItem.CMD_SET,key,value_prefix + i));
                	//test.enqueue(new QueueItem(QueueItem.CMD_DELETE,key));
                	i++;
                }
                try{
            		long end = System.currentTimeMillis();
            		double taken = ((double)(end-start)/1000);
            		double item_perSec = ((double)iteration/taken);
            		ovr_time +=  taken;
        			if(AppTest.isVervose())
            		System.out.println(" enqueue override"+iteration+" times in "+taken+" sec ["+item_perSec+" operations pr.sec]");
                }catch(Exception e){
                	
                }
    		}catch(Exception e){
    			e.printStackTrace();
    			fail();
    		}finally{
                try {
					test.compact();
				} catch (AlreadyFinalizedException e) {
				}
    		}
    		

			//Get
    		i =0;
    		try{
    			long start = System.currentTimeMillis();
                while(i<iteration){
                	String key = key_prefix + i;
                	String value =  value_prefix + i;
                	Object item = test.get(key);
                   	assertNotNull(item);
                   	assertTrue(value.compareTo(item.toString())==0);
                   	i++;
                }
                try{
            		long end = System.currentTimeMillis();
            		double taken = ((double)(end-start)/1000);
            		double item_perSec = ((double)iteration/taken);
            		get_time +=  taken;
        			if(AppTest.isVervose())
            		System.out.println(" get "+iteration+" times in "+taken+" sec ["+item_perSec+" operations pr.sec]");
                }catch(Exception e){
                	
                }
    		}catch(Exception e){
    			e.printStackTrace();
    			fail();
    		}finally{
                try {
					test.compact();
				} catch (AlreadyFinalizedException e) {
				}
    		}
    		
			//Dequeue + Commit
    		i =0;
    		try{
    			long start = System.currentTimeMillis();
                while(i<iteration){
                	String key = key_prefix + i;
                	String value =  value_prefix + i;
                	QueueItem item =  test.dequeue();
                	if(item!=null){
                    	assertTrue(key.compareTo(item.key())==0);
                    	assertTrue(value.compareTo(item.value().toString())==0);
                    	item.commit();
                    	assertNull(test.get(key));
                	}
                	i++;
                }
                try{
            		long end = System.currentTimeMillis();
            		double taken = ((double)(end-start)/1000);
            		double item_perSec = ((double)iteration/taken);
            		deq_time +=  taken;
        			if(AppTest.isVervose())
            		System.out.println(" dequeue -> commit "+iteration+" times in "+taken+" sec ["+item_perSec+" operations pr.sec]");
                }catch(Exception e){
                	
                }
    		}catch(Exception e){
    			e.printStackTrace();
    			fail();
    		}finally{
                try {
					test.compact();
				} catch (AlreadyFinalizedException e) {
				}
    		}
    		

    		
			try {
				test.clear(false);
			} catch (AlreadyFinalizedException e) {
			}
			time++;
			
		}
		if(AppTest.isVervose())
		System.out.println(">>>>>>>>");
		double  enq_avr = (((double)enq_time/sampling_iteration));
		System.out.println("enqueue total="+(double)enq_time+"sec average="+enq_avr+"sec");
		double  ovr_avr = (((double)ovr_time/sampling_iteration));
		System.out.println("enqueue override total="+(double)ovr_time+"sec average="+ovr_avr+"sec");
		double  get_avr = (((double)get_time/sampling_iteration));
		System.out.println("get total="+(double)get_time+"sec average="+get_avr+"sec");
		double  deq_avr = (((double)deq_time/sampling_iteration));
		System.out.println("dequeue -> commit total="+(double)deq_time+"sec average="+deq_avr+"sec");
		
	}
	@Test
	public void testConsitency(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
			KVQueue test = new KVQueueLockingImpl();
			
			long start = System.currentTimeMillis();
	        try {
	    		int i = 0;
	    		while(i<iteration){
	            	String key = key_prefix + i;
	            	String value = value_prefix + i;
	            	QueueItem item = new QueueItem(QueueItem.CMD_DELETE,key,value);
	            	
	            	//Enqueue(New)
	            	test.enqueue(item);
	            	assertEquals(value,(String) test.get(key));
	            	
	            	//Enqueue(Update)
	            	String value_2 = value_prefix+"_2_" + i;
	            	QueueItem item_2 = new QueueItem(QueueItem.CMD_DELETE,key,value_2);
	            	test.enqueue(item_2);
	            	assertEquals(value_2,(String) test.get(key));
	            	
	            	//Put(Update)
	            	String value_3 = value_prefix+"_3_" + i;
	            	QueueItem item_3 = new QueueItem(QueueItem.CMD_DELETE,key,value_3);
	            	test.put(key,item_3);
	            	assertEquals(value_3,(String) test.get(key));
	            	
	            	//Dequeue
	            	assertEquals(item_3,test.dequeue());
	            	
	            	//Commit
	            	item_3.commit();
	            	assertNull(test.get(key));

	            	i++;
	            }
				iterated = i;
	    		
	    		long end = System.currentTimeMillis();
	    		double taken = ((double)(end-start)/1000);
	    		System.out.println(" enq -> enq -> put -> deq -> out "+iteration+" items in "+taken+" sec. ");
			} catch (Exception e) {
				e.printStackTrace();
			}
			assertTrue(true);
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}
		
	}
	@Test
	public void testMultiThread_Enqueue_Dequeue(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
			KVQueue test = new KVQueueLockingImpl();
			
			long start = System.currentTimeMillis();
	        for(int i=0; i<thread_multiply; i++){
	    		EnqueueThread enq_t = new EnqueueThread(test);
	            m_executor_service.execute(enq_t);
	    		DequeueThread deq_t = new DequeueThread(test);
	            m_executor_service.execute(deq_t);
	        }
	        try {
				deq_latch.await();
	    		long end = System.currentTimeMillis();
	    		double taken = ((double)(end-start)/1000);
	    		System.out.println(" enq -> deq "+iteration+" items by "+thread_multiply+" threads in "+taken+" sec. ");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			assertTrue(true);
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}

		
	}
	@Test
	public void testMultiThread_Enqueue_Out(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
			KVQueue test = new KVQueueLockingImpl();
			
			long start = System.currentTimeMillis();
	        for(int i=0; i<thread_multiply; i++){
	    		EnqueueThread enq_t = new EnqueueThread(test);
	            m_executor_service.execute(enq_t);
	            OutThread out_t = new OutThread(test);
	            m_executor_service.execute(out_t);
	        }
	        try {
				out_latch.await();
	    		long end = System.currentTimeMillis();
	    		double taken = ((double)(end-start)/1000);
	    		System.out.println(" enq -> out "+iteration+" items by "+thread_multiply+" threads in "+taken+" sec. ");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			assertTrue(true);
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}
		
	}
	@Test
	public void testMultiThread_Enqueue_Dequeue_Out(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
			KVQueue test = new KVQueueLockingImpl();
			
			long start = System.currentTimeMillis();
	        for(int i=0; i<thread_multiply; i++){
	    		EnqueueThread enq_t = new EnqueueThread(test);
	            m_executor_service.execute(enq_t);
	            DequeueThread deq_t = new DequeueThread(test);
	            m_executor_service.execute(deq_t);
	            OutThread out_t = new OutThread(test);
	            m_executor_service.execute(out_t);
	        }
	        try {
				out_latch.await();
	    		long end = System.currentTimeMillis();
	    		double taken = ((double)(end-start)/1000);
	    		System.out.println(" enq -> deq -> out "+iteration+" items by "+thread_multiply+" threads in "+taken+" sec. ");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			assertTrue(true);
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}

		
	}
	private class EnqueueThread extends Thread{
		KVQueue test = null;
       	private volatile boolean run = true;
       	public EnqueueThread(){}
        public EnqueueThread(KVQueue test){
        	super("EnqueueThread");
			this.test=test;
			run = true;
		}
		public void run(){
			/*
	         * Test iteration benchmark first to last
	         */
			try{
    			long i =0;
                while(run&&enq_latch.getCount()>0){
                	i = enq_latch.getCount();
                	String key = Long.toHexString(random.nextLong());
                	String value =  value_prefix + i;
                	QueueItem item = new QueueItem(QueueItem.CMD_SET, key,value); 
            		test.enqueue(item);
                	if(AppTest.isVervose())
                	System.out.println("("+this.hashCode()+")+++++"+i+" ["+key+":"+value+"]");
                	kvs.enqueue(new QueueItem(0,key,value));
                	item.unlock();
                	enq_latch.countDown();
                }
    		}catch(Exception e){
    			e.printStackTrace();
    			AppTest.cancelLatch(enq_latch);
    			fail();
    		}
        	if(AppTest.isVervose())
        	System.out.println("("+this.hashCode()+")+++++Finished size["+test.queueSize()+"]");
		}
	}
	private class DequeueThread extends Thread{
		KVQueue test = null;
	   	private volatile boolean run = true;
    	private ThreadFreeReadWriteLock lock_obj = null;
    	public DequeueThread(){}
       	public DequeueThread(KVQueue test){
        	super("DequeueThread");
			this.test=test;
			run = true;
    		this.lock_obj = new ThreadFreeReadWriteLock(true);
		}
		public void run(){
			/*
	         * Test iteration benchmark first to last
	         */
			try{
    			long i =0;
                while(run&&deq_latch.getCount()>0){
                	i = deq_latch.getCount();
                	QueueItem item = test.dequeue();
                	if(kvs!=null&&item!=null){
                		if(!kvs.contains(item.key)){
                        	if(AppTest.isVervose())
                			System.out.println("Already out just after dequeued. "+item.toString());
                		}
                    	//assertTrue();
                    	if(item!=null){
                        	item.unlock();
                        	if(AppTest.isVervose())
                        	System.out.println("("+this.hashCode()+")-----"+i+" ["+item.key+":"+item.value+"]");
                    	}
                	}
                	deq_latch.countDown();
                }
    		}catch(Exception e){
    			e.printStackTrace();
    			AppTest.cancelLatch(deq_latch);
    			fail();
    		}
        	if(AppTest.isVervose())
        	System.out.println("("+this.hashCode()+")-----Finished size["+test.queueSize()+"]");
		}
	}
	private class OutThread extends Thread{
		KVQueue test = null;
	   	private volatile boolean run = true;
	   	public OutThread(){}
       	public OutThread(KVQueue test){
        	super("OutThread");
			this.test=test;
			run = true;
		}
		public void run(){
			/*
	         * Test iteration benchmark first to last
	         */
			try{
    			long i =0;
                while(run&&out_latch.getCount()>0){
                	i = out_latch.getCount();
                	synchronized(kvs){
                		if(kvs.queueSize()==0){
                			AppTest.cancelLatch(out_latch);
                			break;
                		}
                	}
                	QueueItem quedItem = kvs.dequeue();
                	if(quedItem!=null){
                    	quedItem.commit();
                    	//assertTrue(kvs.contains(item.key));
                    	if(AppTest.isVervose())
                            System.out.println("("+this.hashCode()+")>>>>>"+i+" ["+quedItem.key()+":"+quedItem.value+"]");
                        	out_latch.countDown();
                	}
                }
    		}catch(Exception e){
    			e.printStackTrace();
    			AppTest.cancelLatch(out_latch);
    			fail();
    		}
        	if(AppTest.isVervose())
        	System.out.println("("+this.hashCode()+")>>>>>Finished size["+test.queueSize()+"]");
		}
	}

}
