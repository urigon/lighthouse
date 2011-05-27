package jp.co.fujisan.lighthouse;

import static org.junit.Assert.*;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCodingUtils {
	
	private static Random random =AppTest.random;
	private static final int iteration =AppTest.getLoopCount();
	
	public TestCodingUtils(){
		
	}
	
	@Before
	public void setUp() throws Exception {
	}
	
	@After
	public void tearDown() throws Exception{
		
	}
	
	@Test
	public void testCompressAndExtractByDeflate(){
		
		String prefix = "someranfdomevaluestringsarenotspecifiedyetbutifeedyousomestringswahtever111020349521756478625829756927264736875687653";
		try{
			for(int i=0;i<iteration;i++){
				String uncompressed = prefix + Long.toHexString(random.nextLong());
				String compressed = CodingUtils.compress(uncompressed, CodingUtils.CODE_COMPRESSION_TYPE_DEFLATE);
				assertNotNull(compressed);
				assertEquals(uncompressed,CodingUtils.extract(compressed, CodingUtils.CODE_COMPRESSION_TYPE_DEFLATE));			
			}
		}catch(Exception e){
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void testCompressAndExtractByGZIP(){
		
		String prefix = "someranfdomevaluestringsarenotspecifiedyetbutifeedyousomestringswahtever111020349521756478625829756927264736875687653";
		try{
			for(int i=0;i<iteration;i++){
				String uncompressed = prefix + Long.toHexString(random.nextLong());
				String compressed = CodingUtils.compress(uncompressed, CodingUtils.CODE_COMPRESSION_TYPE_GZIP);
				assertNotNull(compressed);
				assertEquals(uncompressed,CodingUtils.extract(compressed, CodingUtils.CODE_COMPRESSION_TYPE_GZIP));			
			}
		}catch(Exception e){
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testInvalidCompressionType(){
		String prefix = "someranfdomevaluestringsarenotspecifiedyetbutifeedyousomestringswahtever111020349521756478625829756927264736875687653";
		try{
			String uncompressed = prefix + Long.toHexString(random.nextLong());
			String compressed = CodingUtils.compress(uncompressed, -1);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
