package jp.co.fujisan.lighthouse.hashring.index;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;


public class MD5IndexGenerator implements IndexGenerator {

	public final static int slice = 4;
	
	private static MessageDigest m_digester = null;;
    
	public MD5IndexGenerator() throws NoSuchAlgorithmException{
		if(m_digester==null){
			m_digester = MessageDigest.getInstance("MD5");
		}
	}
	
	@Override
	public long genIndex(String key) throws Exception {
		byte[] bKey;
		synchronized(m_digester){
			m_digester.reset();
			m_digester.update( key.getBytes() );
			bKey = m_digester.digest();
		}
		long res = ((long)(bKey[3]&0xFF) << 24) | ((long)(bKey[2]&0xFF) << 16) | ((long)(bKey[1]&0xFF) << 8) | (long)(bKey[0]&0xFF);
		return res;
	}
	
	@Override
	public long[] genNodeIndex(String key,int replicas) throws Exception {
		
		List<Long> index_list = new ArrayList<Long>();
		
		int factor = replicas/slice;
		m_digester.reset();
		for ( int j = 0; j < factor; j++ ) {
			byte[] d = m_digester.digest( ( key + "-" + j ).getBytes() );
			
			for ( int h = 0 ; h < slice; h++ ) {
				Long index = 
					  ((long)(d[3+h*4]&0xFF) << 24)
					| ((long)(d[2+h*4]&0xFF) << 16)
					| ((long)(d[1+h*4]&0xFF) << 8)
					| ((long)(d[0+h*4]&0xFF));
				
				index_list.add(index);
			}
		}
		
		long[] ret = new long[replicas];
		
		for(int i=0;i<index_list.size();i++){
			ret[i] = index_list.get(i);
		}
		
		return ret;
	}


	
    private int[] slice( byte[] Bytes,int slice )
    {
        if( Bytes == null )
            return null;

        int curByte;
        String byteStr;
        int num = Bytes.length;
        StringBuffer sb = new StringBuffer();
        for( int i = 0; i < num; i++ )
        {
            curByte = (int)Bytes[i];
            if( curByte < 0 )
                curByte += 256;
            byteStr = Integer.toHexString( curByte );
            //byteStr = Integer.toString( curByte );
            if( byteStr.length() <= 1 )
                sb.append("0");
            sb.append( byteStr );
        }
        
        byte[] b_key = sb.toString().getBytes();
        int _slice = b_key.length/4;
        int[] indexes= new int[_slice];
        for(int i=0;i<_slice;i++)
        {
        	indexes[i] = (( b_key[3+i*4] << 24) |(b_key[2+i*4] << 16)|(b_key[1+i*4] << 8) | b_key[0+i*4] );
        }

        return indexes;
    }

	@Override
	public int getSlice() {
		// TODO Auto-generated method stub
		return slice;
	}


}
