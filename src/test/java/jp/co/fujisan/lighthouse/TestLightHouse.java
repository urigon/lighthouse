package jp.co.fujisan.lighthouse;

import static org.junit.Assert.*;

import java.io.File;
import java.security.KeyStore.Entry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import jp.co.fujisan.lighthouse.LightHouse;


public class TestLightHouse{
	
    private static final String ring_id = "test_ring";
	private static final String key_prefix = "key-";
	private static final String value_prefix = "value-";
	private static final int iteration = AppTest.getLoopCount();
	private static boolean vervose = AppTest.isVervose();
	
	private static LightHouse test = null;
	public TestLightHouse(){
		
	}
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
	}

	@Before
	public void setUp() throws Exception {

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
    		Iterator<String> ite = test.keys("").iterator();
    		while(ite.hasNext()){
				try {
					test.delete(ite.next());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
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
		System.out.println("====>Passed\r\n\r\n");
	}

	@Test
	public void testConstructor(){
		
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
		try {
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));
    	}catch(Exception e){
    			e.printStackTrace();
    			fail(e.getMessage());
	    }finally{
			AppTest.end(1);
		}
		
	}
	
	@Test
	public void testStart(){
		
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
		try {
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));
			test.start();
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(1);
		}
        try {
			test.start();
			fail();
		} catch (Exception e) {
	        assertTrue(true);
		}
	}

	@Test
	public void testCreationClientFailureonAddNode(){
		
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
		try {
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        test.setRing_id(ring_id);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));
			test.start();
			AppTest.addServer(test,  "success");
			AppTest.addServer(test,  "failure");
			fail();
    	}catch(Exception e){
            assertTrue(true);
    	}finally{
			AppTest.end(1);
		}
        assertTrue(true);
	
	}
	
	@Test
	public void testSync(){
		int iterated =0;
		try {
			
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        
	        //setup
	        test.setRing_id(ring_id);
			test.setCommit_async(false);
			test.setReplica_number(0);
			test.setGet_retry_number(0);
	        test.setup();
	        
			assertEquals(test,LightHouse.getInstance(ring_id));
			
			//start and add storage
			test.start();
			AppTest.addServer(test,"debug_1");
			
			//Perform test
			AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());
			iterated = sequencialCRUD(test,iteration);
			
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(iterated);
		}

	}
	
	@Test
	public void testAsync(){
		int iterated =0;
		try {
			
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        
	        //setup
	        test.setRing_id(ring_id);
	        test.setCommit_async(true);
	        test.setAsync_thread_number(3);
	        test.setReplica_number(0);
			test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));
			
			//start and add storage
			test.start();
			AppTest.addServer(test,"debug_1");
			AppTest.addServer(test,"debug_2");
			AppTest.addServer(test,"debug_3");
			AppTest.addServer(test,"debug_4");
			
			//Perform test
			AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	
			iterated = sequencialCRUD(test,iteration);
			
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(iterated);
		}
	}
	
	@Test
	public void testSyncReplication(){
		int iterated =0;
		try {
			
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        
	        //setup
	        test.setRing_id(ring_id);
	        test.setCommit_async(false);
	        test.setReplica_number(3);
			test.setGet_retry_number(3);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));
			
			//start and add storage
			test.start();
			AppTest.addServer(test,"debug_1");
			AppTest.addServer(test,"debug_2");
			AppTest.addServer(test,"debug_3");
			AppTest.addServer(test,"debug_4");
			
			//Perform test
			AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
			iterated = sequencialCRUD(test,iteration);
			
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(iterated);
		}

	}

	@Test
	public void testAsyncReplication(){
		int iterated =0;
		try {
			
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        
	        //setup
	        test.setRing_id(ring_id);
	        test.setCommit_async(true);
	        test.setReplica_number(3);
			test.setGet_retry_number(3);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));
			
			//start and add storage
			test.start();
			AppTest.addServer(test,"debug_1");
			AppTest.addServer(test,"debug_2");
			AppTest.addServer(test,"debug_3");
			AppTest.addServer(test,"debug_4");
			
			//Perform test
			AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
			iterated = sequencialCRUD(test,iteration);
			
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(iterated);
		}
	}

	@Test
	public void testCompressionByDeflate(){
		int iterated =0;
		try {
			
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        
	        //setup
	        test.setRing_id(ring_id);
			test.setCommit_async(false);
			test.setReplica_number(0);
			test.setGet_retry_number(0);
			test.setCompress_values(true);
			test.setCompression_type(CodingUtils.VALUE_COMPRESSION_TYPE_DEFLATE);
			test.setCompression_threshold(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));
			
			//start and add storage
			test.start();
			AppTest.addServer(test,"debug_1");
			
			//Perform test
			AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
			iterated = sequencialCRUD(test,iteration);
			
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(iterated);
		}

	}

	@Test
	public void testCompressionByGZIP(){
		int iterated =0;
		try {
			
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        
	        //setup
	        test.setRing_id(ring_id);
			test.setCommit_async(false);
			test.setReplica_number(0);
			test.setGet_retry_number(0);
			test.setCompress_values(true);
			test.setCompression_type(CodingUtils.VALUE_COMPRESSION_TYPE_GZIP);
			test.setCompression_threshold(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));
			
			//start and add storage
			test.start();
			AppTest.addServer(test,"debug_1");
			
			//Perform test
			AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
			iterated = sequencialCRUD(test,iteration);
			
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(iterated);
		}

	}

	@Test
	public void testClientNativeTyrant(){
		int iterated =0;
		try {
			
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        
	        //setup
	        test.setRing_id(ring_id);
			test.setCommit_async(false);
			test.setReplica_number(0);
			test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));
			
			//start and add storage
			test.start();
			AppTest.addServer(test,"live_native_tyrant_1");
			
			//Perform test
			AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
			iterated = sequencialCRUD(test,iteration);
			
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(iterated);
		}
	}

	@Test
	public void testClientMemcached(){
		int iterated =0;
		try {
			
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        
	        //setup
	        test.setRing_id(ring_id);
			test.setCommit_async(false);
			test.setReplica_number(0);
			test.setGet_retry_number(0);
	        test.setup();
			assertEquals(test,LightHouse.getInstance(ring_id));
			
			//start and add storage
			test.start();
			AppTest.addServer(test,"live_mem_1");
			
			//Perform test
			AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
			iterated = sequencialCRUD(test,iteration);
			
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(iterated);
		}
	}
	
	@Test
	public void testKeys(){
		int iterated =0;
		String group = "group";
		List<String> key_collector = new LinkedList<String>(); 
		try {
			
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        
	        //setup
	        test.setRing_id(ring_id);
			test.setCommit_async(false);
			test.setReplica_number(2);
			test.setGet_retry_number(0);
	        test.setup();
	        
			assertEquals(test,LightHouse.getInstance(ring_id));
			
			//start and add storage
			test.start();
			AppTest.addServer(test,"debug_1");
			AppTest.addServer(test,"debug_2");
			AppTest.addServer(test,"debug_3");
			AppTest.addServer(test,"debug_4");
			
			//Perform test
			AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());
	        for (iterated=0;iterated < iteration; iterated++) {
	        	String key = key_prefix+iterated;
	        	String value = value_prefix +iterated;
				test.set(group, key, value);
				key_collector.add(key);
	        }
	        {
				Set<String> keys = test.keys(group,null);
				assertEquals(iterated,keys.size());
				Iterator<String> ite = key_collector.iterator();
				while(ite.hasNext()){
					String key = ite.next();
					assertTrue(keys.contains(key));
				}
	        }
	        {
				Set<String> keys = test.keys(group,key_prefix);
				assertEquals(iterated,keys.size());
				Iterator<String> ite = key_collector.iterator();
				while(ite.hasNext()){
					String key = ite.next();
					assertTrue(keys.contains(key));
				}
	        }
	        {
				Set<String> keys = test.keys(group);
				assertEquals(iterated,keys.size());
				Iterator<String> ite = keys.iterator();
				while(ite.hasNext()){
					String key = ite.next();
					assertNotNull(test.get(key));
				}
	        }
	        {
				Set<String> keys = test.keys(null);
				assertEquals(iterated,keys.size());
				Iterator<String> ite = keys.iterator();
				while(ite.hasNext()){
					String key = ite.next();
					assertNotNull(test.get(key));
				}
	        }
			
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(iterated);
		}

	}
	
	@Test
	public void testGetAll(){
		int iterated =0;
		String group = "group";
		Map<String,Object> collector = new HashMap<String,Object>();
		try {
			
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        
	        //setup
	        test.setRing_id(ring_id);
			test.setCommit_async(false);
			test.setReplica_number(2);
			test.setGet_retry_number(0);
	        test.setup();
	        
			assertEquals(test,LightHouse.getInstance(ring_id));
			
			//start and add storage
			test.start();
			AppTest.addServer(test,"debug_1");
			AppTest.addServer(test,"debug_2");
			AppTest.addServer(test,"debug_3");
			AppTest.addServer(test,"debug_4");
			
			//Perform test
			AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());
	        for (iterated=0;iterated < iteration; iterated++) {
	        	String key = key_prefix+iterated;
	        	String value = value_prefix +iterated;
				test.set(group, key, value);
				collector.put(key,value);
	        }
	        {
				Map<String,Object> result = test.getGroup(group);
				assertEquals(iterated,result.size());
				Iterator<Map.Entry<String,Object>> ite = result.entrySet().iterator();
				while(ite.hasNext()){
					Map.Entry<String,Object> entry = ite.next();
					String key = entry.getKey();
					Object value = entry.getValue();
					assertEquals(collector.get(key),value);
				}
	        }
	        {
				Map<String,Object> result = test.getGroup(group,key_prefix);
				assertEquals(iterated,result.size());
				Iterator<Map.Entry<String,Object>> ite = result.entrySet().iterator();
				while(ite.hasNext()){
					Map.Entry<String,Object> entry = ite.next();
					String key = entry.getKey();
					Object value = entry.getValue();
					assertEquals(collector.get(key),value);
				}
	        }
			
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(iterated);
		}

	}

	@Test
	public void testLock(){
		int iterated =0;
		String group = "group";
		List<String> key_collector = new LinkedList<String>(); 
		try {
			
			test = AppTest.loadTestInstance("Lighthouse_not_configured");
	        assertNotNull(test);
	        
	        //setup
	        test.setRing_id(ring_id);
			test.setCommit_async(false);
			test.setReplica_number(2);
			test.setGet_retry_number(0);
			test.setGlobal_lock_enable(true);
			test.setGlobal_lock_local_bind("1976");
			test.setGlobal_lock_timeout_millis(0);
			test.setGlobal_lock_remote_hosts("192.168.12.109:1222");
	        test.setup();
	        
			assertEquals(test,LightHouse.getInstance(ring_id));
			
			//start and add storage
			test.start();
			AppTest.addServer(test,"debug_1");
			AppTest.addServer(test,"debug_2");
			AppTest.addServer(test,"debug_3");
			AppTest.addServer(test,"debug_4");
			
			//Perform test
			AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());
			test.lock("key");
			
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(iterated);
		}

	}

	private static int sequencialCRUD(LightHouse test,int iterate)throws Exception
	{
		int iterated=0;
        for (iterated=0;iterated < iterate; iterated++) {
        	String key = key_prefix+iterated;
        	String value = value_prefix +iterated;
        	//1. Create
	    	value += "1. Create";
	    	Long add = test.set(key, value);
        	assertTrue(add>=0);

	    	//2. Read
	    	assertEquals(value, test.get(key));
	    	
	    	//3. Update
	    	value += "3. Update";
        	assertTrue(test.set(key, value)>=0);
	    	assertEquals(value, test.get(key));
	    	
	    	//4. Delete key
        	assertTrue(test.delete(key));
	    	
	    	//4-2. Read after delete key
	    	assertNull(test.get(key));

	    	//4-3. Delete after delete key
	    	if(test.isCommit_async()){
	    		assertTrue(test.delete(key));
	    	}else{
	    		assertFalse(test.delete(key));
	    	}
	    	assertNull(test.get(key));

			//4-4. Update after delete key
	    	value += "4-4. Update after delete key";
        	assertTrue(test.set(key, value)>=0);
	    	assertEquals(value, test.get(key));
	    	
	    	//5. Delete key
    		assertTrue(test.delete(key));
	    	assertNull(test.get(key));
        }
        return iterated;
	}	
}
