package jp.co.fujisan.lighthouse;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.StopWatch;

import jp.co.fujisan.lighthouse.client.ClientFactory;
import jp.co.fujisan.lighthouse.client.MemcachedClient;

import tokyotyrant.RDB;

public class AppTest {
	
	public static final Random random = new Random();
	public static int loopCount = 1000;

	public static Properties testcase_properties = new Properties();
	public static Properties storage_properties = new Properties();
	
	public static String spring_beans_filename;    
    
	public static String test_lighthouse_configuration_path = "./lighthouse.properties";
	public static String test_lighthouse_storage_path = "./storage.xml";

	public static String test_lighthouse_configuration_temp_path = "./temp/lighthouse.properties";
	public static String test_lighthouse_storage_temp_path = "./temp/storage.xml";
	
	static{
		try{
	        InputStream is = AppTest.class.getClassLoader().getResourceAsStream("testcase.properties");
	        testcase_properties = new Properties();
	        testcase_properties.load( is );
	        is.close();
	        
	        spring_beans_filename= testcase_properties.getProperty("spring-beans_filename");
	        System.out.println("[testcase.properties]:spring-beans_filename=" + spring_beans_filename );

	        loopCount = Integer.parseInt(testcase_properties.getProperty("loopCount"));
	        System.out.println("[testcase.properties]:loopCount=" + loopCount);
	        
	        test_lighthouse_configuration_path = testcase_properties.getProperty("lighthouse.config.path");
	        System.out.println("[testcase.properties]:test_lighthouse_configuration_path=" + test_lighthouse_configuration_path);

	        test_lighthouse_storage_path = testcase_properties.getProperty("lighthouse.storage.path");
	        System.out.println("[testcase.properties]:test_lighthouse_storage_path=" + test_lighthouse_storage_path);
			
	        test_lighthouse_configuration_temp_path = testcase_properties.getProperty("lighthouse.config.temp.path");
	        System.out.println("[testcase.properties]:test_lighthouse_configuration_temp_path=" + test_lighthouse_configuration_temp_path);

	        test_lighthouse_storage_temp_path = testcase_properties.getProperty("lighthouse.storage.temp.path");
	        System.out.println("[testcase.properties]:test_lighthouse_storage_temp_path=" + test_lighthouse_storage_temp_path);
	        
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
	        InputStream is = AppTest.class.getClassLoader().getResourceAsStream("teststorages.properties");
	        storage_properties = new Properties();
	        storage_properties.load( is );
	        is.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public AppTest(){
		
	}
	public static boolean isVervose(){
		return Boolean.parseBoolean(testcase_properties.getProperty("vervose"));
	}
	
	public static int getLoopCount()
	{
		return loopCount;
	}
	
	public static LightHouse loadTestInstance(String beanId){
        ApplicationContext context = new ClassPathXmlApplicationContext(spring_beans_filename);
        return (LightHouse)context.getBean(beanId);
	}
	
	private static Map<String,Object> parseStoragePropertiesItem(String name){
		Map<String,Object> result = new HashMap<String,Object>();

		try{
			String prop = storage_properties.getProperty(name);
			String[] prop_array = prop.split(",");
			for(int i=0;i<prop_array.length;i++){
				switch(i){
				case 0:
					result.put("type",prop_array[i]);
					break;
				case 1:
					result.put("weight",Integer.parseInt(prop_array[i]));
					break;
				case 2:
					result.put("ip",prop_array[i]);
					break;
				case 3:
					result.put("port",Integer.parseInt(prop_array[i]));
					break;
				default:
					throw new Exception();
				}
			}
		}catch(Exception e){
			
		}
		return result;

		
	}
	public static int addServer(LightHouse test , String name) throws Exception{
		Integer server_id = 0;
		Map<String,Object> conf = parseStoragePropertiesItem(name);
		String type = (String)conf.get("type");
		String ip =  (String)conf.get("ip");
		int weight = (Integer)conf.get("weight");
		int port =(Integer)conf.get("port");
		try {
			server_id = test.addServer(type, name, weight, ip, port,false );
			if(server_id==null||server_id==0){
				throw new Exception("Failed to add server.");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			throw e;
		}
		return server_id;

	}
	
	public final static void clearTyrantStorage(String name,List<String> keys){
		Map<String,Object> conf = parseStoragePropertiesItem(name);
		String type = (String)conf.get("type");
		String ip =  (String)conf.get("ip");
		int port =(Integer)conf.get("port");
		
		if(ClientFactory.SERVER_TYPE_TYRANT.compareToIgnoreCase(type)==0){
			RDB rdb = new RDB();
			try{
				rdb.open(new InetSocketAddress(ip,port));
				Iterator<String> ite = keys.iterator();
				while(ite.hasNext()){
					try{
						rdb.out(ite.next());
					}catch(Exception e){
					}
				}
			}catch(Exception e){
			}finally{
				rdb.close();
			}
		}
	}
	public final static void clearMemcachedStorage(String name,List<String> keys){
		Map<String,Object> conf = parseStoragePropertiesItem(name);
		String type = (String)conf.get("type");
		String ip =  (String)conf.get("ip");
		int port =(Integer)conf.get("port");
		
		if(ClientFactory.SERVER_TYPE_MEMCACHED.compareToIgnoreCase(type)==0){
			MemcachedClient client = null;
			try {
				client = new MemcachedClient(ip,port);
				Iterator<String> ite = keys.iterator();
				while(ite.hasNext()){
					try{
						client.delete(ite.next());
					}catch(Exception e){
					}
				}
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}finally{
				if(client!=null){
					client.terminate();
				}
			}
		}
	}
	
	public final static void cancelLatch(CountDownLatch latch){
		while(latch.getCount()>0){
			latch.countDown();
		}
	}
	
    private static StopWatch s = null;
	public static final void begin(Class clazz,StackTraceElement e[]){
		System.out.print(getCurrentMethodName(clazz,e)+"\r\n");
        s = new StopWatch();
        s.start();
	}
	public static final void end(int iterated){
        s.stop();
        printResultTime(s,iterated);
	}
	
    public static void printResultTime(StopWatch watch,int loopCount){
		double taken = ((double)watch.getTotalTimeMillis());
		String item_perSec = null;
		try{
			item_perSec = new BigDecimal((double)loopCount/(double)(taken/1000)).setScale(2, RoundingMode.HALF_UP).toString();
		}catch(java.lang.NumberFormatException e){
			item_perSec = "Infinite";
		}
		System.out.print(" " +loopCount+" times in "+taken+" .msec ["+item_perSec+" per.sec]");
    }
    
	public static String getCurrentMethodName(Class clazz,StackTraceElement e[]) {
		   boolean doNext = false;
		   for (StackTraceElement s : e) {
		       if (doNext) {
		          return clazz.getName() +"#" + s.getMethodName();
		       }
		       doNext = s.getMethodName().equals("getStackTrace");
		   }
		   return null;
	 }


}
