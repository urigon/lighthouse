package jp.co.fujisan.lighthouse;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import static org.junit.Assert.*

import org.gmock.WithGMock
import org.junit.runner.RunWith

import org.spockframework.runtime.Sputnik
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.StopWatch;

import spock.lang.*

@WithGMock
@RunWith(Sputnik)
class LightHouseSpec extends Specification{

	StopWatch stopWatch = new StopWatch()
    def vervose = true
	String spring_beans_file_path,config_file_path,storage_file_path=null;
    LightHouse lighthouse = null;
	int loopCount = 0
    def setup(){

	        InputStream is = new FileInputStream(new File("src/test/resources/testcase.properties"));
	        Properties testcase_properties = new Properties();
	        testcase_properties.load( is );
	        is.close();
	        
	        spring_beans_file_path= testcase_properties.getProperty("spring-beans_filename");
	        System.out.println("[testcase.properties]:spring-beans_filename=" + spring_beans_file_path );  
			
	        config_file_path= testcase_properties.getProperty("lighthouse.config.temp.path");
	        System.out.println("[testcase.properties]:lighthouse.config.temp.path=" + config_file_path );  
			
	        storage_file_path= testcase_properties.getProperty("lighthouse.storage.temp.path");
	        System.out.println("[testcase.properties]:lighthouse.storage.temp.path=" + storage_file_path );  
			
	        loopCount= Integer.parseInt(testcase_properties.getProperty("loopCount"));
	        System.out.println("[testcase.properties]:loopCount=" + loopCount );  
			
			stopWatch.start()
    }
    def cleanup(){
		lighthouse.terminate();
		stopWatch.stop()
		println stopWatch.prettyPrint()

		File noneed = new File(config_file_path);
		noneed.delete();
		System.out.println("lighthouse.config.temp.path["+config_file_path+"] deleted.");  
		noneed = new File(storage_file_path);
		noneed.delete();
		System.out.println("lighthouse.storage.temp.path["+storage_file_path+"] deleted.");  
		
    }
    
	private LightHouse loadBean(String beanId){
		ApplicationContext context = new ClassPathXmlApplicationContext(spring_beans_file_path);
		return (LightHouse)context.getBean(beanId);
	}
	
	private Map<String,Object> loadStorageProperties(String name){
		
		Map<String,Object> result = new HashMap<String,Object>();

		try{
	        InputStream is = new FileInputStream(new File("src/test/resources/teststorages.properties"));
	        Properties storage_properties = new Properties();
	        storage_properties.load( is );
	        is.close();
			
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
	
	public int addServer(LightHouse lighthouse , String name) throws Exception{
		Integer server_id = 0;
		Map<String,Object> conf = loadStorageProperties(name);
		String type = (String)conf.get("type");
		String ip =  (String)conf.get("ip");
		int weight = (Integer)conf.get("weight");
		int port =(Integer)conf.get("port");
		try {
			server_id = lighthouse.addServer(type, name, weight, ip, port,false );
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

	private  int sequencialCRUD(LightHouse lighthouse,int iterate)throws Exception
	{
		Configurations configurations = lighthouse.getConfiguraions();
		int iterated=0;
		for (iterated=0;iterated < iterate; iterated++) {
			String key = "key_"+iterated;
			String value = "value_" +iterated;
			//1. Create
			value += "1. Create";
			Long add = lighthouse.set(key, value);
			assertTrue(add>=0);

			//2. Read
			if(lighthouse.get(key)==null){
				String va = lighthouse.get(key);
			}
			assertEquals(value, lighthouse.get(key));
			
			//3. Update
			value += "3. Update";
			assertTrue(lighthouse.set(key, value)>=0);
			assertEquals(value, lighthouse.get(key));
			
			//4. Delete key
			assertTrue(lighthouse.delete(key));
			
			//4-2. Read after delete key
			assertNull(lighthouse.get(key));

			//4-3. Delete after delete key
			if(configurations.isCommit_async()){
				assertTrue(lighthouse.delete(key));
			}else{
				assertFalse(lighthouse.delete(key));
			}
			assertNull(lighthouse.get(key));

			//4-4. Update after delete key
			value += "4-4. Update after delete key";
			assertTrue(lighthouse.set(key, value)>=0);
			assertEquals(value, lighthouse.get(key));
			
			//5. Delete key
			assertTrue(lighthouse.delete(key));
			assertNull(lighthouse.get(key));
		}
		return iterated;
	}


    def "SpringBeansコンストラクタでインスタンスを生成する"(){
        when:
        lighthouse = loadBean("Lighthouse_configured");
        
        then:
        lighthouse
        lighthouse.getStatus() == LightHouse.STATUS_STOP
		Configurations configurations = lighthouse.getConfiguraions()
		configurations.getRing_id() == "test_ring";
		
    }
    
    def "インスタンスをスタートする"(){
        setup:
        lighthouse = loadBean("Lighthouse_configured");
        
		when:
		lighthouse.start()
		
        then:
		lighthouse.getStatus() == LightHouse.STATUS_RUNNING
		Configurations configurations = lighthouse.getConfiguraions()
		configurations.getRing_id() == "test_ring";
    }
	
	def "ノード追加時にクライアント作成でエラーが発生したら例外をスローする"(){
		setup:
		lighthouse = loadBean("Lighthouse_configured");
		
		when:
		lighthouse.start()
		lighthouse.getStatus() == LightHouse.STATUS_RUNNING
		
		and:
		addServer(lighthouse,"failure");		
		
		then:
		thrown(Exception)
		
	}
	
	def "シングルスレッドによる同期モードでのCRUDテスト"(){
		setup:
		lighthouse = loadBean("Lighthouse_configured");
		Configurations configurations = lighthouse.getConfiguraions();
		configurations.setCommit_async(false);
		lighthouse.setup();
		
		when:
		lighthouse.start()
		lighthouse.getStatus() == LightHouse.STATUS_RUNNING
		
		and:
		addServer(lighthouse,"debug_1");
		
		then:
		loopCount == sequencialCRUD(lighthouse,loopCount);
		
	}

	def "シングルスレッドによる同期モード、4ノードレプリケーションでのCRUDテスト"(){
		setup:
		lighthouse = loadBean("Lighthouse_configured");
		Configurations configurations = lighthouse.getConfiguraions();
		configurations.setCommit_async(false);
		configurations.setReplica_number(3);
		configurations.setGet_retry_number(3);
		lighthouse.setup();
		
		when:
		lighthouse.start()
		lighthouse.getStatus() == LightHouse.STATUS_RUNNING
		
		and:
		addServer(lighthouse,"debug_1");
		addServer(lighthouse,"debug_2");
		addServer(lighthouse,"debug_3");
		addServer(lighthouse,"debug_4");
		
		then:
		loopCount == sequencialCRUD(lighthouse,loopCount);
		
	}
	
	def "シングルスレッドによる非同期モード、４ノードレプリケーションでのCRUDテスト"(){
		setup:
		lighthouse = loadBean("Lighthouse_configured");
		Configurations configurations = lighthouse.getConfiguraions();
		configurations.setCommit_async(false);
		configurations.setReplica_number(3);
		configurations.setGet_retry_number(3);
		lighthouse.setup();
		
		when:
		lighthouse.start()
		lighthouse.getStatus() == LightHouse.STATUS_RUNNING
		
		and:
		addServer(lighthouse,"debug_1");
		addServer(lighthouse,"debug_2");
		addServer(lighthouse,"debug_3");
		addServer(lighthouse,"debug_4");
		
		then:
		loopCount == sequencialCRUD(lighthouse,loopCount);
		
	}
	
	def "DEFLATEによる圧縮を有効にしたCRUDテスト"(){
		setup:
		lighthouse = loadBean("Lighthouse_configured");
		Configurations configurations = lighthouse.getConfiguraions();
		configurations.setCommit_async(false);
		configurations.setReplica_number(0);
		configurations.setGet_retry_number(0);
		configurations.setCompress_values(true);
		configurations.setCompression_type(CodingUtils.VALUE_COMPRESSION_TYPE_DEFLATE);
		configurations.setCompression_threshold(0);
		lighthouse.setup();
		
		when:
		lighthouse.start()
		lighthouse.getStatus() == LightHouse.STATUS_RUNNING
		
		and:
		addServer(lighthouse,"debug_1");
		
		then:
		loopCount == sequencialCRUD(lighthouse,loopCount);
		
	}
	def "GZIPによる圧縮を有効にしたCRUDテスト"(){
		setup:
		lighthouse = loadBean("Lighthouse_configured");
		Configurations configurations = lighthouse.getConfiguraions();
		configurations.setCommit_async(false);
		configurations.setReplica_number(0);
		configurations.setGet_retry_number(0);
		configurations.setCompress_values(true);
		configurations.setCompression_type(CodingUtils.VALUE_COMPRESSION_TYPE_GZIP);
		configurations.setCompression_threshold(0);
		lighthouse.setup();
		
		when:
		lighthouse.start()
		lighthouse.getStatus() == LightHouse.STATUS_RUNNING
		
		and:
		addServer(lighthouse,"debug_1");
		
		then:
		loopCount == sequencialCRUD(lighthouse,loopCount);
		
	}
	def "TokyoTyrantストレージノードへ接続してのCRUDテスト"(){
		setup:
		lighthouse = loadBean("Lighthouse_configured");
		Configurations configurations = lighthouse.getConfiguraions();
		lighthouse.setup();
		
		when:
		lighthouse.start()
		lighthouse.getStatus() == LightHouse.STATUS_RUNNING
		
		and:
		addServer(lighthouse,"live_native_tyrant_1")
		
		then:
		loopCount == sequencialCRUD(lighthouse,loopCount);
		
	}
	def "Memcachedストレージノードへ接続してのCRUDテスト"(){
		setup:
		lighthouse = loadBean("Lighthouse_configured");
		Configurations configurations = lighthouse.getConfiguraions();
		lighthouse.setup();
		
		when:
		lighthouse.start()
		lighthouse.getStatus() == LightHouse.STATUS_RUNNING
		
		and:
		addServer(lighthouse,"live_mem_1")
		
		then:
		loopCount == sequencialCRUD(lighthouse,loopCount);
		
	}

	def "KEYコマンドテスト"(){
		setup:
		lighthouse = loadBean("Lighthouse_configured");
		Configurations configurations = lighthouse.getConfiguraions();
		configurations.setCommit_async(false);
		configurations.setReplica_number(3);
		configurations.setGet_retry_number(3);
		lighthouse.setup();
		lighthouse.start()
		lighthouse.getStatus() == LightHouse.STATUS_RUNNING
		addServer(lighthouse,"debug_1");
		addServer(lighthouse,"debug_2");
		addServer(lighthouse,"debug_3");
		addServer(lighthouse,"debug_4");
		def key_collector = []
		for (def iterated=0;iterated < loopCount; iterated++) {
			String key = "key_"+iterated;
			String value = "value_" +iterated;
			lighthouse.set("group", key, value);
			key_collector.add(key);
		}

		when:"グループのみ指定して属するキーを取得する"
		def keys = lighthouse.keys("group",null);
		
		then:""
		assert key_collector.size() == keys.size()
		for(key in keys){
			assert key_collector.contains(key)
		}
		
		when:"グループとキープリフィックスを指定して属するキーを取得する"
		keys = lighthouse.keys("group","key_");
		
		then:""
		assert key_collector.size() == keys.size()
		for(key in keys){
			assert key_collector.contains(key)
		}

		when:"取得したキーのエントリーが存在するか？"
		keys = lighthouse.keys("group",null);
		
		then:""
		assert key_collector.size() == keys.size()
		for(key in keys){
			assert lighthouse.get("group",key)
		}
		
		when:"取得したキーのエントリーが存在するか？"
		keys = lighthouse.keys(null);
		
		then:""
		assert key_collector.size() == keys.size()
		for(key in keys){
			assert lighthouse.get(key)
		}
		
	}
	
	def "getGROUPコマンドテスト"(){
		setup:
		lighthouse = loadBean("Lighthouse_configured");
		Configurations configurations = lighthouse.getConfiguraions();
		configurations.setCommit_async(false);
		configurations.setReplica_number(3);
		configurations.setGet_retry_number(3);
		lighthouse.setup();
		lighthouse.start()
		lighthouse.getStatus() == LightHouse.STATUS_RUNNING
		addServer(lighthouse,"debug_1");
		addServer(lighthouse,"debug_2");
		addServer(lighthouse,"debug_3");
		addServer(lighthouse,"debug_4");
		Map<String,Object> entry_collector = new HashMap<String,Object>()
		for (def iterated=0;iterated < loopCount; iterated++) {
			String key = "key_"+iterated;
			String value = "value_" +iterated;
			lighthouse.set("group", key, value);
			entry_collector.put(key,value);
		}

		when:"グループのみ指定してエントリーを取得する"
		def entries = lighthouse.getGroup("group");
		
		then:""
		assert entry_collector.size() == entries.size()
		for(entry in entries){
			assert entry_collector.get(entry.getKey()) == entry.getValue()
		}
		
		when:"グループとキーサフィックスを指定してエントリーを取得する"
		entries = lighthouse.getGroup("group","key_");
		
		then:""
		assert entry_collector.size() == entries.size()
		for(entry in entries){
			assert entry_collector.get(entry.getKey()) == entry.getValue()
		}

	}

}
