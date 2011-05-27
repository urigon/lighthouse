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
import java.util.SortedMap;
import java.util.TreeMap;

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
import junit.framework.TestCase;


public class TestHashRing{
	
	private List<Client> client_list = null;
	private static final Random random = new Random();
	private static boolean vervose = AppTest.isVervose();
	public TestHashRing(){}
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
	public void testGenerateCircle(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
	        HashRing test = new HashRing(client_list);
			
	        test.dump();
	        
	        test.clear();
	        
	        assertTrue(true);
//			Node node_1 = test.getNode(name_base+"_1");
//			assertNotNull(node_1);
//			assertEquals(name_base+"_1",node_1.getName());
//			assertEquals(server_base+"_1",node_1.getServer());
//			assertEquals(1,node_1.getWeight());
//			assertEquals(Node.NODE_TYPE_TYRENT,node_1.getType());
	//
//			Node node_2 = test.getNode(name_base+"_2");
//			assertNotNull(node_2);
//			assertEquals(name_base+"_2",node_2.getName());
//			assertEquals(server_base+"_2",node_2.getServer());
//			assertEquals(1,node_2.getWeight());
//			assertEquals(Node.NODE_TYPE_TYRENT,node_2.getType());
	//	
//			Node node_3 = test.getNode(name_base+"_3");
//			assertNotNull(node_3);
//			assertEquals(name_base+"_3",node_3.getName());
//			assertEquals(server_base+"_3",node_3.getServer());
//			assertEquals(3,node_3.getWeight());
//			assertEquals(Node.NODE_TYPE_TYRENT,node_3.getType());
	//
//			Node node_4 = test.getNode(name_base+"_4");
//			assertNotNull(node_4);
//			assertEquals(name_base+"_4",node_4.getName());
//			assertEquals(server_base+"_4",node_4.getServer());
//			assertEquals(1,node_4.getWeight());
//			assertEquals(Node.NODE_TYPE_TYRENT,node_4.getType());
	//
//			Node node_5 = test.getNode(name_base+"_5");
//			assertNotNull(node_5);
//			assertEquals(name_base+"_5",node_5.getName());
//			assertEquals(server_base+"_5",node_5.getServer());
//			assertEquals(1,node_5.getWeight());
//			assertEquals(Node.NODE_TYPE_TYRENT,node_5.getType());
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}



	}
	
	@Test
	public void testGetNode_SpreadRatio_with_sequencial_key(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
			if(vervose)
		        System.out.println("-----------------------------------------");
		        HashRing test = new HashRing(client_list);
				
		        //test.dump();
				if(vervose){
					System.out.println(">>>>>Total size ["+test.size()+"]");
					System.out.println(">>>>>Index range ["+test.getFirstIndex()+"-"+test.getLastIndex()+"]");
				}
		        
				SortedMap<Long,Integer> index_map = new TreeMap<Long,Integer>();
				SortedMap<Integer,Integer> client_map = new TreeMap<Integer,Integer>();
		        try{
		            int total =0;
		            while(total<10000){
		            	String key = "KEY_PREFIX"+"-"+total;
		            	Node node_1 = test.getNode(key);
		        		long index = node_1.getIndex();
		        		Client client = node_1.getClient();
		        		Integer client_id = client.getId();
		        		assertNotNull(node_1);
		        		if(!index_map.containsKey(node_1.getIndex())){
		        			index_map.put(index,0);
		        		}
		        		if(!client_map.containsKey(client_id)){
		        			client_map.put(client_id,0);
		        		}
		        		//System.out.println("Got Node["+node_1.getName()+":"+index+"] for KEY["+key+":"+test.genIndex(key)+"]");
		        		int num = index_map.get(index);
		        		index_map.put(index,++num);
		        		num = client_map.get(client_id);
		        		client_map.put(client_id,++num);
		        		total++;
		            }
//		            System.out.println(">>>>>Node Hit ratio");
//		            for(Iterator<Long> itr=index_map.keySet().iterator(); itr.hasNext();){
//		    			Long index = itr.next();
//		    			int num = index_map.get(index);
//		    			double  hit_ratio = (((double)num/total)*100);
//						System.out.println("     ["+index+"]="+hit_ratio+"%");
//		    		}
		    		if(vervose){
		                System.out.println("");
		                System.out.println("");
		                System.out.println(">>>>>Client Hit ratio");
		    		}
		    		double hit_ratio_avr = 0;
		            for(Iterator<Integer> itr=client_map.keySet().iterator(); itr.hasNext();){
		            	Integer index = itr.next();
		    			int num = client_map.get(index);
		    			double  hit_ratio = (((double)num/total)*100);
		    			if(vervose)
		    				System.out.println("     ["+index+"]="+hit_ratio+"%");
		    			hit_ratio_avr += hit_ratio;
		    		}
		            System.out.println(">>>>>Average Hit ratio = "+((double)hit_ratio_avr/client_map.size()));
		            assertTrue(true);
		        }catch(Exception e){
		        	e.printStackTrace();
		        	fail();
		        }
		        if(vervose)
		        	System.out.println("-----------------------------------------");
		        
				
//				Client client_1 = node_1.getClient();
//				String clientIndex = client_1.getId();
//				
//				test.remove(clientIndex, false);
//		        test.dump();
//				
//				node_1 = test.getNode(key);
//				assertNotNull(node_1);
//				System.out.println("Got Node["+node_1.getName()+":"+node_1.getIndex()+"] for KEY["+key+":"+test.genIndex(key)+"]");

				
//				node_1= test.depart("Node_name_1");
//		        test.dump();
//		        
//		        Node node_unknown = test.getNode("Node_name_1");
//				assertNotNull(node_unknown);
//				assertNotSame(node_1,node_unknown);
//				
//		        test.remove("Node_name_3");
//		        test.dump();
		//
//		        node_unknown = test.getNode("Node_name_3",true);
//				assertNull(node_unknown);
		//
//		        node_unknown = test.getNode("Node_name_3",false);
//				assertNotNull(node_unknown);
//				assertNotSame("Node_name_3",node_unknown.getName());
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}

        
	}
	
	@Test
	public void testGetNode_SpreadRatio_with_random_key(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
			if(vervose)
		        System.out.println("-----------------------------------------");
		        HashRing test = new HashRing(client_list);
				
		        //test.dump();
		        if(vervose){
		        	System.out.println(">>>>>Total size ["+test.size()+"]");
		        	System.out.println(">>>>>Index range ["+test.getFirstIndex()+"-"+test.getLastIndex()+"]");
		        }
				SortedMap<Long,Integer> index_map = new TreeMap<Long,Integer>();
				SortedMap<Integer,Integer> client_map = new TreeMap<Integer,Integer>();
		        String key = Long.toHexString(random.nextLong());
		        try{
		            int total =0;
		            while(total<10000){
		                key = Long.toHexString(random.nextLong());
		            	Node node_1 = test.getNode(key);
		        		long index = node_1.getIndex();
		        		Client client = node_1.getClient();
		        		Integer client_id = client.getId();
		        		assertNotNull(node_1);
		        		if(!index_map.containsKey(node_1.getIndex())){
		        			index_map.put(index,0);
		        		}
		        		if(!client_map.containsKey(client_id)){
		        			client_map.put(client_id,0);
		        		}
		        		//System.out.println("Got Node["+node_1.getName()+":"+index+"] for KEY["+key+":"+test.genIndex(key)+"]");
		        		int num = index_map.get(index);
		        		index_map.put(index,++num);
		        		num = client_map.get(client_id);
		        		client_map.put(client_id,++num);
		        		total++;
		            }
//		            System.out.println(">>>>>Node Hit ratio");
//		            for(Iterator<Long> itr=index_map.keySet().iterator(); itr.hasNext();){
//		    			Long index = itr.next();
//		    			int num = index_map.get(index);
//		    			double  hit_ratio = (((double)num/total)*100);
//						System.out.println("     ["+index+"]="+hit_ratio+"%");
//		    		}
		            if(vervose){
		            	System.out.println("");
		                System.out.println("");
		                System.out.println(">>>>>Client Hit ratio");
		            }
		            double hit_ratio_avr = 0;
		            for(Iterator<Integer> itr=client_map.keySet().iterator(); itr.hasNext();){
		            	Integer index = itr.next();
		    			int num = client_map.get(index);
		    			double  hit_ratio = (((double)num/total)*100);
		                if(vervose)
						System.out.println("     ["+index+"]="+hit_ratio+"%");
		                hit_ratio_avr += hit_ratio;
		    		}
		            System.out.println(">>>>>Average Hit ratio = "+((double)hit_ratio_avr/client_map.size()));
		            assertTrue(true);
		        }catch(Exception e){
		        	e.printStackTrace();
		        	fail();
		        }
		        if(vervose)
		        System.out.println("-----------------------------------------");
		        
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}

	}

//	public void testRemoveNode(){
//
//        HashRing test = new HashRing(node_list);
//		
//        test.dump();
//        
//        test.remove("Node_name_1");
//        test.dump();
//        test.remove("Node_name_2");
//        test.dump();
//        test.remove("Node_name_3");
//        test.dump();
//        test.remove("Node_name_4");
//        test.dump();
//        test.remove("Node_name_5");
//        test.dump();
//        assertTrue(true);
//
//	}
	

	@Test
	public void testAccessor(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
	        client_list = new ArrayList<Client>();
	        try{
	            int i =0;
	            while(i<100){
	                client_list.add(new TestServer("Server_name_"+i,1)); 
	            	i++;
	            }
	        }catch(Exception e){
	        	e.printStackTrace();
	        	fail();
	        }
	        
	        HashRing test = null;
	        HashRingIterator ite = null;
	        if(vervose)
	        System.out.println("-----------------------------------------");
	        /*
	         * Test construction benchmark
	         */
	        try{
	    		long start = System.currentTimeMillis();
	            if(vervose)
	    		System.out.println("HashRing>>>>>Start construction ["+start+"]");
	    		test = new HashRing(client_list);
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
	        test.dump();
	        
			long firstIndex = test.getFirstIndex();
			long lastIndex = test.getLastIndex();
			
			long preIndex = test.getPredecessorIndex(firstIndex);
			long sucIndex = test.getSuccessorIndex(lastIndex);		
			
			assertEquals(firstIndex,sucIndex);
			assertEquals(lastIndex,preIndex);
			
			Node firstNode = test.getNode(firstIndex);
			Node lastNode = test.getNode(lastIndex);
			
			Node preNode = test.getPredecessor(firstIndex);
			Node sucNode = test.getSuccessor(lastIndex);
			
			assertEquals(firstNode,sucNode);
			assertEquals(lastNode,preNode);
	        
	        /*
	         * Test iteration benchmark
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
	            if(vervose){
	        		System.out.println("HashRing>>>>>End iteration["+end+"]");
	        		System.out.println("HashRing>>>>>Elapsed Time["+(end-start)+"]");
	            }
	        }catch(Exception e){
	        	e.printStackTrace();
	        	fail();
	        }
	        ite = null;
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
	        
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}

        
	}
	private class TestServer implements Client
	{

		String name;
		Integer id;
		int replicas = 0;

		int weight=1;
		public TestServer(){}
		/**
		 * @param name
		 * @param id
		 * @param host
		 * @throws UnknownHostException 
		 */
		public TestServer(String name,int weight) throws UnknownHostException {
			super();
			this.name = name;
			this.id =  new Random().nextInt();
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
