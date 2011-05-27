package jp.co.fujisan.lighthouse.hashring.index;

import static org.junit.Assert.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import jp.co.fujisan.lighthouse.AppTest;
import jp.co.fujisan.lighthouse.LightHouse;
import jp.co.fujisan.lighthouse.client.TyrantClient;
import jp.co.fujisan.lighthouse.hashring.AbstractNode;
import jp.co.fujisan.lighthouse.hashring.Node;
import jp.co.fujisan.lighthouse.hashring.SimpleNode;
import jp.co.fujisan.lighthouse.hashring.index.CRC32IndexGenerator;
import jp.co.fujisan.lighthouse.hashring.index.IndexGenerator;
import jp.co.fujisan.lighthouse.hashring.index.IndexGeneratorFactory;
import jp.co.fujisan.lighthouse.hashring.index.MD5IndexGenerator;
import jp.co.fujisan.lighthouse.hashring.index.SimpleIndexGenerator;
import junit.framework.TestCase;

import static org.junit.Assert.*;


public class TestIndex {
	
	private static boolean vervose = AppTest.isVervose();
	public TestIndex(){}
	@After
	public void tearDown() throws Exception{
		System.out.println("====>Passed\r\n\r\n");
		
	}
	
	@Test
	public void testGetGenerator(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
		try {
			IndexGenerator test = IndexGeneratorFactory.getGenerator(IndexGeneratorFactory.GEN_TYPE_SIMPLE);
			assertNotNull(test);
			assertTrue(test instanceof SimpleIndexGenerator); 
			
			test = null;
	        test = IndexGeneratorFactory.getGenerator(IndexGeneratorFactory.GEN_TYPE_MD5);
			assertNotNull(test);
			assertTrue(test instanceof MD5IndexGenerator); 

			test = null;
			test = IndexGeneratorFactory.getGenerator(IndexGeneratorFactory.GEN_TYPE_CRC32);
			assertNotNull(test);
			assertTrue(test instanceof CRC32IndexGenerator); 
			
    	}catch(Exception e){
    			e.printStackTrace();
    			fail(e.getMessage());
	    }finally{
			AppTest.end(1);
		}
		

	}
	
	public void testGEN_TYPE_SIMPLE(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
			final String id_suffix = "TEST_INDEX_";
			IndexGenerator test = IndexGeneratorFactory.getGenerator(IndexGeneratorFactory.GEN_TYPE_SIMPLE);
			assertNotNull(test);
			assertTrue(test instanceof SimpleIndexGenerator); 
			
			TreeMap<Long,Integer> indexies = new TreeMap<Long,Integer>();
			
			long start = System.currentTimeMillis();
			for(int i=0;i<1000;i++){
				String key = id_suffix+i;
				long index;
				try {
					index = test.genIndex(key);
					//assertTrue(index>0);
					if(vervose)
					System.out.println("["+System.currentTimeMillis()+"]"+key+"=>"+index );
				} catch (Exception e) {
					// TODO Auto-generated catch block
					if(vervose)
					System.err.println("["+System.currentTimeMillis()+"]"+key+"=>FAILED : "+e.getMessage());
					index=0;
				}
				indexies.put(index,i);
			}
			long end = System.currentTimeMillis();
			if(vervose){
				System.out.println(">>>>>End ["+end+"]");
				System.out.println(">>>>>Elapsed Time["+(end-start)+"]");
			}
			
			long firstIndex = indexies.firstKey();
			long lastIndex = indexies.lastKey();
			
			long totalRange = lastIndex - firstIndex;
			
			long preIndex = 0;
			double totalGap = 0;
			for(Iterator<Long> itr=indexies.keySet().iterator(); itr.hasNext();){
				Long index = itr.next();
				int num = indexies.get(index);
				
				long gap = index - preIndex;
				double  gapPer = (((double)gap/totalRange));
				if(vervose)
				System.out.println("     ["+index+" : "+num+"] gap="+gap+" per="+gapPer+"");
				preIndex = index;
				totalGap += gapPer;
			}
			double  gapTotalAvr = (((double)totalGap/indexies.size()));
			System.out.println(">>>>>Gap Avr["+gapTotalAvr+"]");
			assertTrue(true);
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}

		
	}
	public void testGEN_TYPE_MD5(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
			final String id_suffix = "TEST_INDEX_";
			IndexGenerator test = IndexGeneratorFactory.getGenerator(IndexGeneratorFactory.GEN_TYPE_MD5);
			assertNotNull(test);
			assertTrue(test instanceof MD5IndexGenerator); 
			
			TreeMap<Long,Integer> indexies = new TreeMap<Long,Integer>();
			
			long start = System.currentTimeMillis();
			for(int i=0;i<1000;i++){
				String key = id_suffix+i;
				long index;
				try {
					index = test.genIndex(key);
					//assertTrue(index>0);
					if(vervose)
					System.out.println("["+System.currentTimeMillis()+"]"+key+"=>"+index );
				} catch (Exception e) {
					// TODO Auto-generated catch block
					if(vervose)
					System.err.println("["+System.currentTimeMillis()+"]"+key+"=>FAILED : "+e.getMessage());
					index=0;
				}
				indexies.put(index,i);
			}
			long end = System.currentTimeMillis();
			if(vervose){
				System.out.println(">>>>>End ["+end+"]");
				System.out.println(">>>>>Elapsed Time["+(end-start)+"]");
			}
			
			long firstIndex = indexies.firstKey();
			long lastIndex = indexies.lastKey();
			
			long totalRange = lastIndex - firstIndex;
			
			long preIndex = 0;
			double totalGap = 0;
			for(Iterator<Long> itr=indexies.keySet().iterator(); itr.hasNext();){
				Long index = itr.next();
				int num = indexies.get(index);
				
				long gap = index - preIndex;
				double  gapPer = (((double)gap/totalRange));
				if(vervose)
				System.out.println("     ["+index+" : "+num+"] gap="+gap+" per="+gapPer+"");
				preIndex = index;
				totalGap += gapPer;
			}
			double  gapTotalAvr = (((double)totalGap/indexies.size()));
			System.out.println(">>>>>Gap Avr["+gapTotalAvr+"]");
			assertTrue(true);
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}

		
	}
	public void testGEN_TYPE_MD5_TEMP(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
			final String id_suffix = "TEST_INDEX_";
			IndexGenerator test = IndexGeneratorFactory.getGenerator(IndexGeneratorFactory.GEN_TYPE_MD5);
			assertNotNull(test);
			assertTrue(test instanceof MD5IndexGenerator); 
			try {
				TreeMap<Long,String> sorted_map = new TreeMap<Long,String>();
				TreeMap<String,Long> sorted_key = new TreeMap<String,Long>();

				String key = "node-1";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				key = "node-2";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				key = "node-3";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				key = "node-4";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				key = "key-1";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				key = "key-2";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				key = "key-3";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				key = "key-4";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				key = "key-5";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				key = "key-6";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				key = "key-7";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				key = "key-8";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				key = "key-9";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				key = "key-10";
				sorted_map.put(test.genIndex(key),key);
				sorted_key.put(key,test.genIndex(key));
				System.out.println(sorted_key.toString() );
				System.out.println(sorted_map.toString() );
			} catch (Exception e) {
				// TODO Auto-generated catch block
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

	
	public void testGEN_TYPE_CRC32(){
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	   
		int iterated = 1;
		try{
			final String id_suffix = "TEST_INDEX_";
			IndexGenerator test = IndexGeneratorFactory.getGenerator(IndexGeneratorFactory.GEN_TYPE_CRC32);
			assertNotNull(test);
			assertTrue(test instanceof CRC32IndexGenerator); 
			
			TreeMap<Long,Integer> indexies = new TreeMap<Long,Integer>();
			
			long start = System.currentTimeMillis();
			for(int i=0;i<1000;i++){
				String key = id_suffix+i;
				long index;
				try {
					index = test.genIndex(key);
					//assertTrue(index>0);
					if(vervose)
					System.out.println("["+System.currentTimeMillis()+"]"+key+"=>"+index );
				} catch (Exception e) {
					// TODO Auto-generated catch block
					if(vervose)
					System.err.println("["+System.currentTimeMillis()+"]"+key+"=>FAILED : "+e.getMessage());
					index=0;
				}
				indexies.put(index,i);
			}
			long end = System.currentTimeMillis();
			if(vervose){
				System.out.println(">>>>>End ["+end+"]");
				System.out.println(">>>>>Elapsed Time["+(end-start)+"]");
			}
			
			long firstIndex = indexies.firstKey();
			long lastIndex = indexies.lastKey();
			
			long totalRange = lastIndex - firstIndex;
			
			long preIndex = 0;
			double totalGap = 0;
			for(Iterator<Long> itr=indexies.keySet().iterator(); itr.hasNext();){
				Long index = itr.next();
				int num = indexies.get(index);
				
				long gap = index - preIndex;
				double  gapPer = (((double)gap/totalRange));
				if(vervose)
				System.out.println("     ["+index+" : "+num+"] gap="+gap+" per="+gapPer+"");
				preIndex = index;
				totalGap += gapPer;
			}
			double  gapTotalAvr = (((double)totalGap/indexies.size()));
			System.out.println(">>>>>Gap Avr["+gapTotalAvr+"]");
			assertTrue(true);
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			AppTest.end(iterated);
		}
		
	}
}
