package jp.co.fujisan.lighthouse.lock;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
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

import jp.co.fujisan.lighthouse.queue.exception.LockFailureException;
import jp.co.fujisan.lighthouse.queue.exception.LockTimeoutException;

@WithGMock
@RunWith(Sputnik)
class GlobalLockSpec extends Specification{

	 StopWatch stopWatch = new StopWatch()
    def vervose = true
	String spring_beans_filename = null;
	
	def servers,clients
	
    def setup(){

	        InputStream is = new FileInputStream(new File("src/test/resources/testcase.properties"));
	        Properties testcase_properties = new Properties();
	        testcase_properties.load( is );
	        is.close();
	        
	        spring_beans_filename= testcase_properties.getProperty("spring-beans_filename");
	        System.out.println("[testcase.properties]:spring-beans_filename=" + spring_beans_filename );  
			
			stopWatch.start()
    }
    def cleanup(){
        stopWatch.stop()
        println stopWatch.prettyPrint()
		
		servers.each{it.destroy()}
		clients.each{it.destroy()}
		
    }
    
	private GlobalLockServer loadGlobalLockServer(){
		ApplicationContext context = new ClassPathXmlApplicationContext(spring_beans_filename);
		return (GlobalLockServer)context.getBean("GlobalLockServer");   
	}
	private GlobalLockClientManager loadGlobalLockClientManager(){
		ApplicationContext context = new ClassPathXmlApplicationContext(spring_beans_filename);
		return  (GlobalLockClientManager)context.getBean("GlobalLockClientManager");
	}
	
	
    def "シングルスレッドの2つのロックリクエストを1つのサーバーにしてお互いが排他できることを確認する"(){
        setup:
        GlobalLockServer server = new GlobalLockServer(new InetSocketAddress("localhost",1976))
		server.setExipreTimeout(1000)
		server.setRequestTimeout(500)
		server.startup()
		servers = [server];
		GlobalLockClientManager client = new GlobalLockClientManager([new InetSocketAddress("localhost",1976)] as InetSocketAddress[])
		clients = [client]
		
		def token_1 = 1
		def key = "key"
		def token_2 = 2
		
		when:"すでに1がロックを取得中に"
		client.lock(token_1, key);
		
		and:"2がロックを要求すると"
		client.lock(token_2, key)

		then: "LockTimeoutException が投げられる"
		thrown(LockTimeoutException)

		when:"1のロックが解除されると"
		token_1 == client.unlock(token_1,key)
		
		then:"2のロックが取得できる"
		client.lock(token_2, key)
		token_2 == client.unlock(token_2,key)
		
    }
    
    def "ロックがExpireしたことをクライアントが確認できるか"(){
        setup:
        GlobalLockServer server = new GlobalLockServer(new InetSocketAddress("localhost",1976))
		server.setExipreTimeout(100)
		server.startup()
		servers = [server];
		GlobalLockClientManager client = new GlobalLockClientManager([new InetSocketAddress("localhost",1976)] as InetSocketAddress[])
		clients = [client]
        
		def token_1 = 1
		def key = "key"
		
		when:"ロックを取得"
		client.lock(token_1, key);
		
		then: "ロックは正常に解除される(取得時のトークンが帰ってくる)"
		token_1 == client.unlock(token_1,key)
		
		when:"こんどはロックを取得してから"
		client.lock(token_1, key);
		
		and:"expireを待つ"
		synchronized(this){
			wait(200)
		}

		and:"1のロックが解除されると"
		def result = client.unlock(token_1,key)
		
		then: "LockFailureException が投げられる"
		thrown(LockFailureException)
		!result
    }
}
