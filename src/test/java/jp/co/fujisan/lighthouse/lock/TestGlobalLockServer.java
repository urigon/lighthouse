package jp.co.fujisan.lighthouse.lock;

import static org.junit.Assert.*;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
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
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import jp.co.fujisan.lighthouse.AppTest;
import jp.co.fujisan.lighthouse.LightHouse;


public class TestGlobalLockServer{
	
    private static final String ring_id = "test_ring";
	private static final String key_prefix = "key-";
	private static final String value_prefix = "value-";
	private static final int iteration = AppTest.getLoopCount();
	private static boolean vervose = AppTest.isVervose();
	ApplicationContext context = new ClassPathXmlApplicationContext(AppTest.spring_beans_filename);
	
	private static GlobalLockServer test = null;
	public TestGlobalLockServer(){
		
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
	}

	@After
	public void tearDown() throws Exception{

		test = null;
		System.out.println("====>Passed\r\n\r\n");
	}

	@Test
	public void testStart(){
		
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
		try {
			test =  (GlobalLockServer)context.getBean("GlobalLockServer");   

	        assertNotNull(test);
	        test.startup();
	        
			Socket socket = new Socket("localhost",1234); 
			request(socket,"lock?key=きー");
			
			String ret = response(socket);
			System.out.println("レスポンス="+ret);
			
			socket.close();

			socket = new Socket("localhost",1234); 
			request(socket,"lock?key=きー&group=グループ");
			
			ret = response(socket);
			System.out.println("レスポンス="+ret);
			
			socket.close();
			
			test.destroy();
	        
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(1);
		}
	}
	
	@Test
	public void testExpire(){
		
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
		try {
			test =  (GlobalLockServer)context.getBean("GlobalLockServer");   

	        assertNotNull(test);
	        test.startup();
	        
			
			/*
	         * リクエストタイムアウトより先にロックがExpireする
	         */
	        {
	        	Socket socket = new Socket("localhost",1234); 
	        	
		        test.setExipreTimeout(10);
		        test.setRequestTimeout(100);
		        
				request(socket,"lock?key=きー");
				
				String ret = response(socket);
				System.out.println("レスポンス="+ret);
				assertEquals("ok",ret);
				

				socket = new Socket("localhost",1234); 
				request(socket,"lock?key=きー");
				ret = response(socket);
				System.out.println("レスポンス="+ret);
				assertEquals("ok",ret);
	        	
	        }
	        
	        synchronized(this){
	        	wait(1000);
	        }

			
			test.destroy();
	        
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(1);
		}
	}
	
	@Test
	public void testTimeout(){
		
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
		try {
			test =  (GlobalLockServer)context.getBean("GlobalLockServer");   

	        assertNotNull(test);
	        test.startup();
	        
			
			/*
	         * Expireより先にリクエストタイムアウトする
	         */
	        {
	        	Socket socket = new Socket("localhost",1234); 
	        	
		        test.setExipreTimeout(1000);
		        test.setRequestTimeout(10);
		        
				request(socket,"lock?key=きー");
				
				String ret = response(socket);
				System.out.println("レスポンス="+ret);
				assertEquals("ok",ret);

				socket = new Socket("localhost",1234); 
				request(socket,"lock?key=きー");
				ret = response(socket);
				System.out.println("レスポンス="+ret);
				assertEquals("timeout",ret);
	        	
	        }
	        
	        synchronized(this){
	        	wait(1000);
	        }

			
			test.destroy();
	        
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(1);
		}
	}
	
	@Test
	public void testUnlock(){
		
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
		try {
			test =  (GlobalLockServer)context.getBean("GlobalLockServer");   

	        assertNotNull(test);
	        test.startup();
	        
			/*
	         * リクエストタイムアウトする前にUnlockする
	         */
	        {
	        	Socket socket = new Socket("localhost",1234); 
	        	
		        test.setExipreTimeout(1000);
		        test.setRequestTimeout(10);
		        
				request(socket,"lock?key=きー");
				
				String ret = response(socket);
				System.out.println("レスポンス="+ret);
				assertEquals("ok",ret);

				socket = new Socket("localhost",1234); 
				request(socket,"unlock?key=きー");
				ret = response(socket);
				System.out.println("レスポンス="+ret);
				assertEquals("ok",ret);
	        	
				socket = new Socket("localhost",1234); 
				request(socket,"lock?key=きー");
				ret = response(socket);
				System.out.println("レスポンス="+ret);
				assertEquals("ok",ret);
				
				socket.close();
	        }
	        
	        synchronized(this){
	        	wait(1000);
	        }

			
			test.destroy();
	        
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(1);
		}
	}
	
	@Test
	public void testIteration(){
		
		AppTest.begin(this.getClass(),Thread.currentThread().getStackTrace());	    	
		try {
			test =  (GlobalLockServer)context.getBean("GlobalLockServer");   

	        assertNotNull(test);
	        test.startup();
	        
	        int iterated = 0;
	        for (iterated=0;iterated < iteration; iterated++) {
	        	
				/*
		         * リクエストタイムアウトする前にUnlockする
		         */
		        {
		        	Socket socket = new Socket("localhost",1234); 
		        	
			        test.setExipreTimeout(1000);
			        test.setRequestTimeout(10);
			        
					request(socket,"lock?key=きー"+iterated);
					String ret = response(socket);
					System.out.println("レスポンス="+ret);
					assertEquals("ok",ret);
					socket.close();

					socket = new Socket("localhost",1234); 
					request(socket,"unlock?key=きー"+iterated);
					ret = response(socket);
					System.out.println("レスポンス="+ret);
					assertEquals("ok",ret);
					socket.close();
		        }

		        synchronized(this){
		        	wait(1);
		        }

	        }

	        
	        synchronized(this){
	        	wait(1000);
	        }

			
			test.destroy();
	        
    	}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
    	}finally{
			AppTest.end(1);
		}
	}
	
	private static void request(Socket socket, String token)throws Exception{

		OutputStream out = new BufferedOutputStream( socket.getOutputStream());
		out.write( token.getBytes() );
		out.write( "\r\n".getBytes() );
		out.flush();
		
	}

	private static String response(Socket socket)throws Exception{

		BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		String ret = in.readLine();
		return ret;
		
	}

}
