package jp.co.fujisan.lighthouse.lock;

import static org.junit.Assert.*;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
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


public class TestGlobalLockClientManager{
	
    private static final String ring_id = "test_ring";
	private static final String key_prefix = "key-";
	private static final String value_prefix = "value-";
	private static final int iteration = AppTest.getLoopCount();
	private static boolean vervose = AppTest.isVervose();
	ApplicationContext context = new ClassPathXmlApplicationContext(AppTest.spring_beans_filename);
	
	private static GlobalLockClientManager test = null;
	public TestGlobalLockClientManager(){
		
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
			test =  (GlobalLockClientManager)context.getBean("GlobalLockClientManager");   
			//GlobalLockServer server =  (GlobalLockServer)context.getBean("GlobalLockServer");   

	        //assertNotNull(server);
	        //server.startup();
	        
	        assertNotNull(test);
	        InetSocketAddress[] remote_addresses = new InetSocketAddress[1];
	        remote_addresses[0] = new InetSocketAddress("192.168.12.106",1976);
	        test.setRemoteAddresses(remote_addresses);
	        test.startup();
	        
	        test.lock("きー");
	        test.unlock("きー");
			
			test.destroy();
			//server.destroy();
	        
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
