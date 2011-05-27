package jp.co.fujisan.lighthouse.hashring.index;

import java.security.NoSuchAlgorithmException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IndexGeneratorFactory {

	private static final Log logger = LogFactory.getLog(IndexGeneratorFactory.class);
	
	public static final String GEN_TYPE_MD5 = "md5";
	public static final String GEN_TYPE_CRC32 = "crc32";
	public static final String GEN_TYPE_SIMPLE = "simple";
	
	public static final IndexGenerator getGenerator(String key){
		
		try {
			if(GEN_TYPE_MD5.compareToIgnoreCase(key)==0){
				return new MD5IndexGenerator();
			}
			if(GEN_TYPE_CRC32.compareToIgnoreCase(key)==0){
				return new CRC32IndexGenerator();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.debug("<Error>\r\n",e);
		}
		
		return new SimpleIndexGenerator();
		
	}
}
