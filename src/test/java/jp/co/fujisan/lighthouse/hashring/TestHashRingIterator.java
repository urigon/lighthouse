package jp.co.fujisan.lighthouse.hashring;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import jp.co.fujisan.lighthouse.AppTest;
import jp.co.fujisan.lighthouse.client.Client;
import jp.co.fujisan.lighthouse.client.ClientEventListener;
import jp.co.fujisan.lighthouse.client.ClientException;
import jp.co.fujisan.lighthouse.client.TyrantClient;
import jp.co.fujisan.lighthouse.hashring.HashRing;
import jp.co.fujisan.lighthouse.hashring.HashRingIterator;
import jp.co.fujisan.lighthouse.hashring.Node;


public class TestHashRingIterator{
	
	private List<Client> client_list = null;
	private static final Random random = new Random();
	private static boolean vervose = AppTest.isVervose();
	public TestHashRingIterator(){}
	@Before
	public void setUp() throws Exception {
        
        client_list = new ArrayList<Client>();
        
        client_list.add(new TestServer("Server_name_1",1)); 
        client_list.add(new TestServer("Server_name_2",1)); 
        client_list.add(new TestServer("Server_name_3",1)); 
        client_list.add(new TestServer("Server_name_4",1)); 
        client_list.add(new TestServer("Server_name_5",1)); 
        
	}

	@After
	public void tearDown() throws Exception{
		System.out.println("====>Passed\r\n\r\n");
	}
	
	@Test
	public void test(){
		byte b = new Integer(1).byteValue();
		String s1 = Integer.toBinaryString(1);
		String s2 = Integer.toHexString(1);
		String s3 = Integer.toOctalString(1);
		byte[] b1 = s1.getBytes();
		byte[] b2 = s2.getBytes();
		byte[] b3 = s3.getBytes();
		
		assertTrue(true);
	}
	
	
	@Test
	public void testIteration(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
	        //test.dump();
	        HashRing test = this.construction();
	        HashRingIterator ite = null;

	        test.dump();
	        
	        /*
	         * Test iteration benchmark first to last
	         */
	        System.out.println();
	        try{
	        	ite = new HashRingIterator(test);
	    		long start = System.currentTimeMillis();
	    		if(vervose)
	    		System.out.println("HashRing>>>>>Start iteration ["+start+"]");
				Node node = ite.next();
	    		if(vervose)
	    		System.out.println("                      First index ["+node.getIndex()+"]");
	    		while(ite.hasNext()){
	    			node = ite.next();
	    		}
	    		if(vervose)
	    		System.out.println("                      Last index ["+node.getIndex()+"]");
	    		node = ite.next();
	    		assertNull(node);
	    		long end = System.currentTimeMillis();
	    		if(vervose)
	    		System.out.println("HashRing>>>>>End iteration["+end+"]");
	    		if(vervose)
	    		System.out.println("HashRing>>>>>Elapsed Time["+(end-start)+"]");
	        }catch(Exception e){
	        	e.printStackTrace();
	        	fail();
	        }
	        ite = null;
	        
	        /*
	         * Test iteration benchmark last to last
	         */
	        System.out.println();
	        try{
	        	ite = new HashRingIterator(test,test.getLastIndex());
	    		long start = System.currentTimeMillis();
	    		if(vervose)
	    		System.out.println("HashRing>>>>>Start iteration ["+start+"]");
				Node node = ite.next();
	    		if(vervose)
	    		System.out.println("                      First index ["+node.getIndex()+"]");
	    		while(ite.hasNext()){
	    			node = ite.next();
	    		}
	    		if(vervose)
	    		System.out.println("                      Last index ["+node.getIndex()+"]");
	    		node = ite.next();
	    		assertNull(node);
	    		long end = System.currentTimeMillis();
	    		if(vervose)
	    		System.out.println("HashRing>>>>>End iteration["+end+"]");
	    		if(vervose)
	    		System.out.println("HashRing>>>>>Elapsed Time["+(end-start)+"]");
	        }catch(Exception e){
	        	e.printStackTrace();
	        	fail();
	        }
	        ite = null;
	        
	        /*
	         * Test iteration benchmark intermediate to last
	         */
	        System.out.println();
	        try{
	        	ite = new HashRingIterator(test,test.getLastIndex()/2);
	    		long start = System.currentTimeMillis();
	    		if(vervose)
	    		System.out.println("HashRing>>>>>Start iteration ["+start+"]");
				Node node = ite.next();
	    		if(vervose)
	    		System.out.println("                      First index ["+node.getIndex()+"]");
	    		while(ite.hasNext()){
	    			node = ite.next();
	    		}
	    		if(vervose)
	    		System.out.println("                      Last index ["+node.getIndex()+"]");
	    		node = ite.next();
	    		assertNull(node);
	    		long end = System.currentTimeMillis();
	    		if(vervose)
	    		System.out.println("HashRing>>>>>End iteration["+end+"]");
	    		if(vervose)
	    		System.out.println("HashRing>>>>>Elapsed Time["+(end-start)+"]");
	        }catch(Exception e){
	        	e.printStackTrace();
	        	fail();
	        }
	        ite = null;        
	        
	        this.destruction(test);
	        
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}

        
	}
	
	
	@Test
	public void testOneTimeIterationsParallel(){
		
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
			HashRing test = this.construction();
			
			Thread test_1=null,test_2=null,test_3=null;
			try {
				test_1 = new OneTimeIterationThread("F",new HashRingIterator(test));
				test_2 = new OneTimeIterationThread("E",new HashRingIterator(test));
				test_3 = new OneTimeIterationThread("M",new HashRingIterator(test));
			} catch (NothingNodeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			test_1.start();
			test_2.start();
			test_3.start();
			
//			try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
	        this.destruction(test);
			assertTrue(true);
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}

	}
	
	private class OneTimeIterationThread extends Thread{
		String title = "";
		HashRingIterator ite = null;
		public OneTimeIterationThread(){
			
		}
		public OneTimeIterationThread(String title,HashRingIterator test){
			this.title = title;
			ite=test;
		}
		public void run(){
			/*
	         * Test iteration benchmark first to last
	         */
	        System.out.println();
	        try{
	    		long start = System.currentTimeMillis();
	    		if(vervose)
	    		System.out.println(title+">>>>>Start iteration ["+start+"]");
				Node node = ite.next();
				if(node==null){
		    		if(vervose)
		    		System.out.println(title+"                      First index [null]");
				}else{
		    		if(vervose)
		    		System.out.println(title+"                      First index ["+node.getIndex()+"]");
				}
	    		while(ite.hasNext()){
	    			node = ite.next();
	    		}
				if(node==null){
		    		if(vervose)
		    		System.out.println(title+"                      Last index [null]");
				}else{
		    		if(vervose)
		    		System.out.println(title+"                      Last index ["+node.getIndex()+"]");
				}
	    		node = ite.next();
	    		assertNull(node);
	    		long end = System.currentTimeMillis();
	    		if(vervose){
		    		System.out.println(title+">>>>>End iteration["+end+"]");
		    		System.out.println(title+">>>>>Elapsed Time["+(end-start)+"]");
	    		}
	        }catch(Exception e){
	        	e.printStackTrace();
	        	fail();
	        }
	        ite = null;
		}
		
	}
	
	@Test
	public void testIterationsAndRemoveParallel(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
			HashRing test = this.construction();
			
			Thread test_1 = new MultiTimeIterationThread("Iteration_F",test);
			Thread test_2 = new MultiTimeIterationThread("Iteration_E",test);
			Thread test_3 = new MultiTimeIterationThread("Iteration_M",test);
			
			test_1.start();
			test_2.start();
			test_3.start();
			
			Thread test_4 = new RemoveNodeThread("Remove",test);
			test_4.start();
//			try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
	        this.destruction(test);
			assertTrue(true);
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}

	}
	
	private class MultiTimeIterationThread extends Thread{
		String title = "";
		HashRing m_ring = null;
		public MultiTimeIterationThread(){
		}
		public MultiTimeIterationThread(String title,HashRing ring){
			this.title = title;
			m_ring=ring;
		}
		public void run(){
			/*
	         * Test iteration benchmark first to last
	         */
	        System.out.println();
	        HashRingIterator ite = null;
	        try{
	        	while(m_ring.size()!=0){
	        		ite = new HashRingIterator(m_ring);
		    		long start = System.currentTimeMillis();
		    		if(vervose)
		    		System.out.println(title+">>>>>Start iteration ["+start+"]");
					Node node = ite.next();
					if(node==null){
			    		if(vervose)
			    		System.out.println(title+"                      First index [null]");
					}else{
			    		if(vervose)
			    		System.out.println(title+"                      First index ["+node.getIndex()+"]");
					}
		    		while(ite.hasNext()){
		    			node = ite.next();
		    		}
					if(node==null){
			    		if(vervose)
			    		System.out.println(title+"                      Last index [null]");
					}else{
			    		if(vervose)
			    		System.out.println(title+"                      Last index ["+node.getIndex()+"]");
					}
		    		long end = System.currentTimeMillis();
		    		if(vervose){
			    		System.out.println(title+">>>>>End iteration["+end+"]");
			    		System.out.println(title+">>>>>Elapsed Time["+(end-start)+"]");
		    		}
	        	}
	        }catch(Exception e){
	        	e.printStackTrace();
	        	fail();
	        }
	        ite = null;
		}
		
	}
	private class RemoveNodeThread extends Thread{
		String title = "";
		HashRing m_ring = null;
		public RemoveNodeThread(){
			
		}
		public RemoveNodeThread(String title,HashRing ring){
			this.title = title;
			m_ring=ring;
		}
		public void run(){
			/*
	         * Test iteration benchmark first to last
	         */
	        System.out.println();
	        try{
	        	while(m_ring.size()!=0){
	        		long index = m_ring.getFirstIndex();
	        		m_ring.removeNode(index);
		    		if(vervose)
		    		System.out.println(title+">>>>>Removed node ["+index+"] remains nodes ["+m_ring.size()+"]");
	        	}
	        }catch(Exception e){
	        	e.printStackTrace();
	        	fail();
	        }
		}
		
	}
	private HashRing construction(){
		List<Client> clients = new ArrayList<Client>();
        try{
            int i =0;
            while(i<1){
            	clients.add(new TestServer("Server_name_"+i,1)); 
            	i++;
            }
        }catch(Exception e){
        	e.printStackTrace();
        	fail();
        }
        
        HashRing test = null;
		if(vervose)
        System.out.println("-----------------------------------------");
        /*
         * Test construction benchmark
         */
        try{
    		long start = System.currentTimeMillis();
    		if(vervose)
    		System.out.println("HashRing>>>>>Start construction ["+start+"]");
    		test = new HashRing(clients);
    		assertNotNull(test);
    		long end = System.currentTimeMillis();
    		if(vervose){
        		System.out.println("HashRing>>>>>End construction["+end+"]");
        		System.out.println("HashRing>>>>>Elapsed Time["+(end-start)+"]");
        		System.out.println("HashRing>>>>>Total size ["+test.size()+"]");
        		System.out.println("HashRing>>>>>Index range ["+test.getFirstIndex()+"-"+test.getLastIndex()+"]");
    		}
        }catch(Exception e){
        	e.printStackTrace();
        	fail();
        }
        return test;
	}
	
	private void destruction(HashRing test){
		/*
         * Test clear benchmark
         */
        System.out.println();
        try{
    		long start = System.currentTimeMillis();
    		if(vervose)
    		System.out.println("HashRing>>>>>Start clear ["+start+"]");
    		test.clear();
    		long end = System.currentTimeMillis();
    		if(vervose){
        		System.out.println("HashRing>>>>>End clear["+end+"]");
        		System.out.println("HashRing>>>>>Elapsed Time["+(end-start)+"]");
    		}
        }catch(Exception e){
        	e.printStackTrace();
        	fail();
        }
        test = null;
		if(vervose){
	        System.out.println("-----------------------------------------");
	        System.out.println();
	        System.out.println();
		}
	}
	private class TestServer implements Client
	{

		String name;
		Integer id;
		int replicas = 0;

		int weight=1;
		public TestServer(){
			
		}
		/**
		 * @param name
		 * @param id
		 * @param host
		 * @throws UnknownHostException 
		 */
		public TestServer(String name,int weight) throws UnknownHostException {
			super();
			this.name = name;
			this.id = new Random().nextInt();
			this.weight = weight;
		}

		public InetAddress getInetAddress() {
			// TODO Auto-generated method stub
			return null;
		}

		public InetSocketAddress getHost() {
			// TODO Auto-generated method stub
			return null;
		}

		public Integer getId() {
			// TODO Auto-generated method stub
			return this.id;
		}

		public String getName() {
			// TODO Auto-generated method stub
			return this.name;
		}

		public void setHost(String host,int port) throws UnknownHostException {
			// TODO Auto-generated method stub
		}

		public void setId(Integer id) {
			// TODO Auto-generated method stub
			this.id = id;
			
		}

		public void setName(String name) {
			// TODO Auto-generated method stub
			this.name = name;
			
		}
		
		public String toString(){
			return "["+TestServer.class+"]" +
					"name=" + this.name;
		}

		@Override
		public Object get(String key) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean set(String key, Object value) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public int getWeight() {
			// TODO Auto-generated method stub
			return this.weight;
		}

		@Override
		public boolean isAvailable(boolean b) {
			// TODO Auto-generated method stub
			return true;
		}

		@Override
		public boolean setAvailable(boolean available) {
			// TODO Auto-generated method stub
			return true;
		}
		@Override
		public int getCloneNumber() {
			// TODO Auto-generated method stub
			return replicas;
		}

		@Override
		public void setCloneNumber(int clones) {
			// TODO Auto-generated method stub
			replicas = clones;
			
		}

		@Override
		public boolean delete(String key) throws Exception {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public String getType() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Set<String> keys() throws Exception {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getAdded() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getCreated() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ClientException getFailure() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void terminate() {
			// TODO Auto-generated method stub
			
		}

		public boolean delete(String[] keys) throws Exception {
			// TODO Auto-generated method stub
			return false;
		}

		public Map<String, Object> get(String[] keys) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}

		public Set<String> keys(String pattern) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}

		public boolean set(Map<String, Object> entries) throws Exception {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean clear() throws Exception {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public int size() throws Exception {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String getProperty(String key) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getRingId() {
			// TODO Auto-generated method stub
			return null;
		}
		@Override
		public void setClientEventListener(ClientEventListener listener) {
			// TODO Auto-generated method stub
			
		}

		
	}
}
