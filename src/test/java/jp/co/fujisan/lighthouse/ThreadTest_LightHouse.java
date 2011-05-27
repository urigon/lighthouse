package jp.co.fujisan.lighthouse;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import jp.co.fujisan.lighthouse.LightHouse;
import jp.co.fujisan.lighthouse.exception.StatusException;
import jp.co.fujisan.lighthouse.queue.KVQueue;
import jp.co.fujisan.lighthouse.queue.KVQueueSimpleImpl;
import jp.co.fujisan.lighthouse.queue.QueueItem;
import jp.co.fujisan.lighthouse.queue.exception.AlreadyFinalizedException;
import jp.co.fujisan.lighthouse.queue.exception.NoMoreCapacityException;
import junit.framework.TestCase;


public class ThreadTest_LightHouse{
	
    private static final String ring_id = "test_ring";
	private static final String key_prefix = "key-";
	private static final String value_prefix = "value-";
	private static final int iteration = AppTest.getLoopCount();
	private static boolean vervose = AppTest.isVervose();
	private static volatile boolean termination = false;
	private static ExecutorService m_executor_service = null;
	//private static volatile int cnt = 0; 
	private static ThreadKVSForTest kvs = null;
	private static List<String> error_list = null;
	
	private static LightHouse test = null;

	public ThreadTest_LightHouse(){
		
	}
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
	}

	@Before
	public void setUp() throws Exception {
		
		kvs = new ThreadKVSForTest();
		kvs.setQueueLimit(iteration);
		
		m_executor_service = Executors.newCachedThreadPool();
		error_list = Collections.synchronizedList(new ArrayList<String>());
		
		File tmp_conf_file = new File(AppTest.test_lighthouse_configuration_temp_path);
		if(tmp_conf_file.exists()){
			tmp_conf_file.delete();
		}
		
		File tmp_store_file = new File(AppTest.test_lighthouse_storage_temp_path);
		if(tmp_store_file.exists()){
			tmp_store_file.delete();
		}

	}

	@After
	public void tearDown() throws Exception{

		try{
			test.stop(false);
			test.terminate();
		}catch(Exception e){
			
		}
		test = null;
		LightHouse.removeInstance(ring_id);
		File tmp_conf_file = new File(AppTest.test_lighthouse_configuration_temp_path);
		if(tmp_conf_file.exists()){
			tmp_conf_file.delete();
		}
		
		File tmp_store_file = new File(AppTest.test_lighthouse_storage_temp_path);
		if(tmp_store_file.exists()){
			tmp_store_file.delete();
		}

		//cancelLatch(shutdown_latch);
		//m_executor_service.shutdownNow();
		if(error_list.size()>0){
			Iterator<String> ite = error_list.iterator();
			while(ite.hasNext()){
				String err_msg = ite.next();
				System.err.println(err_msg);
			}
			fail();
		}
//		if(!kvs.isEmpty()&&kvs.queueSize()>0&&kvs.cacheSize()>0){
//			Iterator<String> ite = kvs.getQueuedKeys().iterator();
//			while(ite.hasNext()){
//				String err_msg = ite.next();
//				System.err.println("remains key="+err_msg);
//			}
//			fail();
//		}
		error_list.clear();
		error_list = null;
		kvs.clear(false);
		
		System.out.println("====>Passed\r\n\r\n");
	}

	@Test
	public void testSingleThread_Sync_SetGet_SequencialKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			//LightHouse.dump("testSingleThread_Sync_SetGet_SequencialKey",1);
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを1起動
			 * */
            Thread set_thread = new SetThread(test,false);
            m_executor_service.execute(set_thread);
            
            waitForSetCmpl();
            
			/*GETスレッドを1起動
			 * */
			Thread get_thread = new GetThread(test);
            m_executor_service.execute(get_thread);

            waitForGetCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}

		
	}
	@Test
	public void testSingleThread_Sync_SetGet_SequencialKey_Tyrant(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_native_tyrant_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを1起動
			 * */
            Thread set_thread = new SetThread(test,false);
            m_executor_service.execute(set_thread);
            
            waitForSetCmpl();
			
			/*GETスレッドを1起動
			 * */
			Thread get_thread = new GetThread(test);
            m_executor_service.execute(get_thread);

            waitForGetCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}

	}
	@Test
	public void testSingleThread_Async_SetGet_SequencialKey_Tyrant(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(true);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_native_tyrant_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを1起動
			 * */
            Thread set_thread = new SetThread(test,false);
            m_executor_service.execute(set_thread);
            
            waitForSetCmpl();
			
			/*GETスレッドを1起動
			 * */
			Thread get_thread = new GetThread(test);
            m_executor_service.execute(get_thread);

            waitForGetCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}

	}
	@Test
	public void testSingleThread_Async_SetDelete_SequencialKey_Tyrant(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(true);
	        test.setReplica_number(3);
	        test.setGet_retry_number(3);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_native_tyrant_1");
			AppTest.addServer(test,"live_native_tyrant_2");
			AppTest.addServer(test,"live_native_tyrant_3");
			AppTest.addServer(test,"live_native_tyrant_4");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを1起動
			 * */
            Thread set_thread = new SetThread(test,false);
            m_executor_service.execute(set_thread);
            
            waitForSetCmpl();
			
			/*DELETEスレッドを1起動
			 * */
			Thread get_thread = new DeleteThread(test);
            m_executor_service.execute(get_thread);

            waitForDeleteCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}

	}	
	@Test
	public void testSingleThread_Sync_SetMultiGet_SequencialKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを1起動
			 * */
            Thread set_thread = new SetThread(test,false);
            m_executor_service.execute(set_thread);
            
            waitForSetCmpl();
			
			/*MultiGETスレッドを1起動
			 * */
			Thread get_thread = new MultiGetThread(test);
            m_executor_service.execute(get_thread);

            waitForGetCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}

	}
	@Test
	public void testSingleThread_Sync_SetDelete_SequencialKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを1起動
			 * */
            Thread set_thread = new SetThread(test,false);
            m_executor_service.execute(set_thread);
            
            waitForSetCmpl();
			
			/*DELETEスレッドを1起動
			 * */
			Thread delete_thread = new DeleteThread(test);
            m_executor_service.execute(delete_thread);

            waitForDeleteCmpl();
            
		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}
	}
	
	@Test
	public void testSingleThread_Sync_SetGet_RandomKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを1起動
			 * */
            Thread set_thread = new SetThread(test,true);
            m_executor_service.execute(set_thread);
            
            waitForSetCmpl();
			
			/*GETスレッドを1起動
			 * */
			Thread get_thread = new GetThread(test);
            m_executor_service.execute(get_thread);

            waitForDeleteCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}	
	}
	@Test
	public void testSingleThread_Sync_SetMultiGet_RandomKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを1起動
			 * */
            Thread set_thread = new SetThread(test,true);
            m_executor_service.execute(set_thread);
            
            waitForSetCmpl();
			
			/*MultiGETスレッドを1起動
			 * */
			Thread get_thread = new MultiGetThread(test);
            m_executor_service.execute(get_thread);

            waitForDeleteCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}		
	}
	@Test
	public void testSingleThread_Sync_SetDelete_RandomKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを1起動
			 * */
            Thread set_thread = new SetThread(test,true);
            m_executor_service.execute(set_thread);
            
            waitForSetCmpl();
			
			/*DELETEスレッドを1起動
			 * */
			Thread delete_thread = new DeleteThread(test);
            m_executor_service.execute(delete_thread);

            waitForDeleteCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}
	}
	
	@Test
	public void testMultiThread_Sync_SetGet_SequencialKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
	            Thread set_thread = new SetThread(test,false);
	            m_executor_service.execute(set_thread);
			}
			
			/*GETスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
				Thread get_thread = new GetThread(test);
	            m_executor_service.execute(get_thread);
			}

            waitForSetCmpl();
            waitForGetCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}		
	}
	@Test
	public void testMultiThread_Sync_SetMultiGet_SequencialKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
	            Thread set_thread = new SetThread(test,false);
	            m_executor_service.execute(set_thread);
			}
			
			/*MultiGETスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
				Thread get_thread = new MultiGetThread(test);
	            m_executor_service.execute(get_thread);
			}

            waitForSetCmpl();
            waitForGetCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}
		
	}
	@Test
	public void testMultiThread_Sync_SetDelete_SequencialKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
	            Thread set_thread = new SetThread(test,false);
	            m_executor_service.execute(set_thread);
			}
			
			/*DELETEスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
				Thread delete_thread = new DeleteThread(test);
	            m_executor_service.execute(delete_thread);
			}

            waitForSetCmpl();
            waitForDeleteCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}
		
	}
	
	@Test
	public void testMultiThread_Sync_SetGet_RandomKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
	            Thread set_thread = new SetThread(test,true);
	            m_executor_service.execute(set_thread);
			}
			
			/*GETスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
				Thread get_thread = new GetThread(test);
	            m_executor_service.execute(get_thread);
			}

            waitForSetCmpl();
            waitForGetCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}
		
	}
	@Test
	public void testMultiThread_Sync_SetDelete_RandomKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
    		/*SETスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
	            Thread set_thread = new SetThread(test,true);
	            m_executor_service.execute(set_thread);
			}
			
			/*DELETEスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
				Thread delete_thread = new DeleteThread(test);
	            m_executor_service.execute(delete_thread);
			}

            waitForSetCmpl();
            waitForDeleteCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}
		
	}
	
	@Test
	public void testSingleThread_Async_SetGet_RandomKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(true);
	        test.setAsync_thread_number(3);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを1起動
			 * */
            Thread set_thread = new SetThread(test,true);
            m_executor_service.execute(set_thread);
            
            waitForSetCmpl();
			
			/*GETスレッドを1起動
			 * */
			Thread get_thread = new GetThread(test);
            m_executor_service.execute(get_thread);

            waitForGetCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}
	}
	@Test
	public void testSingleThread_Async_SetDelete_RandomKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(true);
	        test.setAsync_thread_number(3);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを1起動
			 * */
            Thread set_thread = new SetThread(test,true);
            m_executor_service.execute(set_thread);
            
            waitForSetCmpl();
			
			/*DELETEスレッドを1起動
			 * */
			Thread delete_thread = new DeleteThread(test);
            m_executor_service.execute(delete_thread);

            waitForDeleteCmpl();

			while(test.getQueueSize()>0&&test.getCacheSize()>0){
				try {
					System.out.println("waiting to be empty caches.");
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}
		
	}
	
	@Test
	public void testMultiThread_Async_SetGet_RandomKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(true);
	        test.setAsync_thread_number(3);
	        test.setSanitize_keys(true);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
	            Thread set_thread = new SetThread(test,true);
	            m_executor_service.execute(set_thread);
			}
			
			/*GETスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
				Thread get_thread = new GetThread(test);
	            m_executor_service.execute(get_thread);
			}

            waitForSetCmpl();
            waitForGetCmpl();

            this.waitForEmptyLightHouseCache();
		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}
		
		
	}
	@Test
	public void testMultiThread_Sync_SetMultiGet_RandomKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setAsync_thread_number(3);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
	            Thread set_thread = new SetThread(test,true);
	            m_executor_service.execute(set_thread);
			}
			
			/*MultiGetスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
				Thread get_thread = new MultiGetThread(test);
	            m_executor_service.execute(get_thread);
			}

            waitForSetCmpl();
            waitForGetCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}
	}
	
	private synchronized void waitForEmptyLightHouseCache(){
		while(test.getQueueSize()>0&&test.getCacheSize()>0){
			try {
				System.out.println("waiting to be empty caches.");
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	@Test
	public void testMultiThread_Async_SetDelete_RandomKey(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(true);
	        test.setAsync_thread_number(3);
	        test.setReplica_number(0);
	        test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			AppTest.addServer(test,"live_debug_1");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
	            Thread set_thread = new SetThread(test,true);
	            m_executor_service.execute(set_thread);
			}
			
			/*DELETEスレッドを3起動
			 * */
			for(int i=0;i<3;i++){
				Thread delete_thread = new DeleteThread(test);
	            m_executor_service.execute(delete_thread);
			}

            waitForSetCmpl();
            waitForDeleteCmpl();
            waitForEmptyLightHouseCache();
            
        }catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}
		
	}
	@Test
	public void testSingleThread_Sync_FailOver(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setAsync_thread_number(0);
	        test.setReplica_number(3);
	        test.setGet_retry_number(6);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			Integer server_id_1 = AppTest.addServer(test,"live_tyrant_1");
			Integer server_id_2 = AppTest.addServer(test,"live_tyrant_2");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを1起動
			 * */
            Thread set_thread = new SetThread(test,false);
            m_executor_service.execute(set_thread);
            
            waitForSetCmpl();
			
			test.removeServer(server_id_2, true, false);
			//LightHouse.dump("testFailOver",1);
			
			/*GETスレッドを1起動
			 * */
			Thread get_thread = new GetThread(test);
            m_executor_service.execute(get_thread);

            waitForGetCmpl();

		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}
		
	}
	
	@Test
	public void testMultiThread_Async_FailOver(){
		try{
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setCommit_async(true);
	        test.setAsync_thread_number(4);
	        test.setReplica_number(3);
	        test.setGet_retry_number(6);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));

			test.start();
			Integer server_id_1 = AppTest.addServer(test,"live_tyrant_1");
			Integer server_id_2 = AppTest.addServer(test,"live_tyrant_2");
			
    		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
			/*SETスレッドを10起動
			 * */
			for(int i=0; i<10; i++){
				Thread set_thread = new SetThread(test,false);
	            m_executor_service.execute(set_thread);
	        }			
			
			test.removeServer(server_id_2, true, false);
			//LightHouse.dump("testFailOver",1);
			
			/*GETスレッドを10起動
			 * */
			for(int i=0; i<10; i++){
				Thread get_thread = new GetThread(test);
	            m_executor_service.execute(get_thread);
	        }			

            waitForSetCmpl();
            waitForGetCmpl();


		}catch(Exception e){
			e.printStackTrace();
			m_executor_service.shutdownNow();
			fail(e.getMessage());
		}finally{
			AppTest.end(iteration);
		}
	}
	
	
	static int cnt = 0;
	private class SetThread extends Thread{
		LightHouse test = null;
        boolean isKeyRundom = false;
        public SetThread(){}
       	public SetThread(LightHouse test,boolean isKeyRundom){
       		this.isKeyRundom = isKeyRundom;
			this.test=test;
		}
		public void run(){
			try{
                while(!termination){
            		if(isKeyRundom){
                    	rundomSet(this.test);
            		}else{
                    	sequencialSet(this.test,++cnt);
            		}
                }
			}catch(NoMoreCapacityException completed){
    		//}catch(StatusException ignore){
    		}catch(Exception e){
    			e.printStackTrace();
    			if(vervose)
	    		error_list.add("+++++"+e.toString());
    			try {
					tearDown();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
    		}
			if(vervose){
	    		System.out.println("+++++"+kvs.getTotalEnqueueCount());
	        	System.out.println("("+this.hashCode()+")+++++Finished.");
			}
		}
	}
	
	private class GetThread extends Thread{
		LightHouse test = null;
        public GetThread(){}
       	public GetThread(LightHouse test){
			this.test=test;
		}
		public void run(){
			try{
                while(!termination&&kvs.getTotalDequeueCount()<kvs.getQueueLimit()){
                	get(this.test);
               }
    		}catch(StatusException ignore){
    		}catch(Exception e){
    			e.printStackTrace();
	    		error_list.add("*****"+e.toString());
    			try {
					tearDown();
				} catch (Exception e1) {
				}
    		}
			if(vervose){
	    		System.out.println("*****"+kvs.getTotalDequeueCount());
	        	System.out.println("("+this.hashCode()+")*****Finished.");
			}
		}
	}
	private class MultiGetThread extends Thread{
		LightHouse test = null;
        public MultiGetThread(){}
       	public MultiGetThread(LightHouse test){
			this.test=test;
		}
		public void run(){
			try{
                while(!termination&&kvs.getTotalDequeueCount()<kvs.getQueueLimit()){
                	mget(this.test);
               }
			}catch(AlreadyFinalizedException ignore){
    		}catch(StatusException ignore){
    		}catch(Exception e){
    			e.printStackTrace();
	    		error_list.add("@@@@@"+e.toString());
    			try {
					tearDown();
				} catch (Exception e1) {
				}
    		}
			if(vervose){
	    		System.out.println("@@@@@"+kvs.getTotalDequeueCount());
	        	System.out.println("("+this.hashCode()+")@@@@@Finished.");
			}
		}
	}
	private class DeleteThread extends Thread{
		LightHouse test = null;
        
		public DeleteThread(){}
       	public DeleteThread(LightHouse test){
			this.test=test;
		}
		public void run(){
			try{
                while(!termination&&kvs.getTotalDequeueCount()<kvs.getQueueLimit()){
                	delete(this.test);
               }
    		}catch(StatusException ignore){
    		}catch(Exception e){
    			e.printStackTrace();
	    		error_list.add("-----"+e.toString());
    			try {
					tearDown();
				} catch (Exception e1) {
				}
    		}
			if(vervose){
	    		System.out.println("-----"+kvs.getTotalDequeueCount());
	        	System.out.println("("+this.hashCode()+")-----Finished.");
			}
		}
	}
	private synchronized void waitForSetCmpl(){
		while(true){
			if(kvs.getTotalEnqueueCount()>=kvs.getQueueLimit()){
				break;
			}
			try {
				wait(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}
		}
	}
	private boolean rundomSet(LightHouse test) throws Exception {
		String key = Long.toHexString(AppTest.random.nextLong());
		String value = value_prefix + key;
		Long ret = test.set(key, value);
		if(ret!=null){
			kvs.enqueue(new QueueItem(0,key, value));
			if(vervose)
	    	System.out.println("+++++["+key+":"+value+"]");
	    	return true;
		}
		return false;
	}
	private boolean sequencialSet(LightHouse test,int cnt) throws Exception{
		String key = key_prefix+":"+cnt;
		String value = value_prefix + cnt;
		Long ret = test.set(key, value);
		if(ret!=null){
			kvs.enqueue(new QueueItem(0,key, value));
			if(vervose)
	    	System.out.println("+++++("+Thread.currentThread().getId()+")["+key+":"+value+"]");
	    	return true;
		}
		return false;
	}
	private synchronized void waitForGetCmpl(){
		while(true){
			if(kvs.queueSize()==0){
				break;
			}
			try {
				wait(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}
		}
	}
	private void get(LightHouse test) throws Exception{
		QueueItem item = kvs.dequeue();
		if(item!=null)
		{
			String key = item.key();
			Object value = item.value();
			if(vervose)
	    	System.out.println("["+key+":"+value+"]");
			Object get_value = test.get(key);
			if(get_value==null){
				if(vervose)
				System.err.println(key+ "=notfound");
	    		error_list.add("*****"+key+ "=notfound");
			}else{
				if(vervose)
			    	System.out.println("*****["+key+":"+get_value+":"+value+"]");
		    	if((get_value!=null&&get_value.toString().compareTo(value.toString())==0)==false){
		    		error_list.add("*****"+key+ "="+get_value+" notmatched with "+value);
		    	}
			}
		}
	}
	private void mget(LightHouse test) throws Exception{
		int unit_size = 10;
		String[] type = new String[unit_size];
		List<String> keys = new ArrayList<String>();
		Map<String,Object> compares = new HashMap<String,Object>();
		while(!kvs.isEmpty()||keys.size()<unit_size)
		{
			QueueItem item = kvs.dequeue();
			if(item!=null)
			{
				String key = item.key();
				Object value = item.value();
				compares.put(key, value);
				if(vervose)
		    	System.out.println("["+key+":"+value+"]");
				keys.add(key);
			}
		}
		if(kvs.queueSize()<unit_size){
			while(kvs.queueSize()>0){
				QueueItem item = kvs.dequeue();
				if(item!=null){
					kvs.remove(item.key());
				}
			}
		}
		Map<String,Object> resuls = test.mget(keys.toArray(type));
		if(resuls==null){
			if(vervose)
			System.err.println(keys.toString()+ "=notfound");
    		error_list.add("@@@@@"+keys.toString()+ "=notfound");
		}else{
			Iterator<String> ite = keys.iterator();
			while(ite.hasNext()){
				String key = ite.next();
				Object value = resuls.get(key);
				if(vervose)
			    	System.out.println("@@@@@["+key+":"+value+":"+compares.get(key)+"]");
			    	if((value!=null&&((String)value).compareTo(((String)compares.get(key)))==0)==false){
			    		error_list.add("@@@@@"+key+ "="+value+" notmatched with "+(String)compares.get(key));
			    	}
			}
		}
	}
	private synchronized void waitForDeleteCmpl(){
		waitForGetCmpl();
	}
	private void delete(LightHouse test) throws Exception{
		QueueItem item = kvs.dequeue();
		if(item!=null)
		{
			String key = item.key();
			try{
				if(!test.delete(key)){
		    		error_list.add("-----"+key+ "failed");
				}
				if(vervose)
		    	System.out.println("-----["+key+"]");
			}catch(AlreadyFinalizedException e){
				
			}
		}
	}
	
}
