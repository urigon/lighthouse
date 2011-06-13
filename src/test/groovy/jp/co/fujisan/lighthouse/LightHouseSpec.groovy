package jp.co.fujisan.lighthouse;

import java.io.IOException;
import java.io.InputStream;
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
	String spring_beans_filename = null;
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
    }
    
	private LightHouse loadBean(String beanId){
		ApplicationContext context = new ClassPathXmlApplicationContext(spring_beans_filename);
		return (LightHouse)context.getBean(beanId);
	}
	
    def "SpringBeansコンストラクタでインスタンスを生成する"(){
        when:
        def lighthouse = loadBean("Lighthouse_configured");
        
        then:
        lighthouse
        lighthouse.getStatus() == LightHouse.STATUS_STOP
		def configurations = lighthouse.getConfiguraions()
		configurations
    }
    
}
