package jp.co.fujisan.lighthouse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;
import java.util.zip.InflaterOutputStream;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CodingUtils {

	private static final Log logger = LogFactory.getLog(CodingUtils.class);

	public final static String VALUE_COMPRESSION_TYPE_DEFLATE = "deflate";	
	public final static String VALUE_COMPRESSION_TYPE_GZIP = "gzip";	
	public final static int CODE_COMPRESSION_TYPE_DEFLATE = 0;	
	public final static int CODE_COMPRESSION_TYPE_GZIP = 1;	
	public final static String[] CONFIG_VALUES_COMPRESSION_TYPES = {VALUE_COMPRESSION_TYPE_DEFLATE,VALUE_COMPRESSION_TYPE_GZIP};

	
	public final static boolean isHandled( Object value ) {

		return (
			value instanceof Byte            ||
			value instanceof Boolean         ||
			value instanceof Integer         ||
			value instanceof Long            ||
			value instanceof Character       ||
			value instanceof String          ||
			value instanceof StringBuffer    ||
			value instanceof Float           ||
			value instanceof Short           ||
			value instanceof Double          ||
			value instanceof Date            ||
			value instanceof StringBuilder   ||
			value instanceof byte[]
			)
		? true
		: false;
    }
	
	public final static byte[] encode( Object value ) throws Exception {
		
		if ( value instanceof Byte )
			return encode( (Byte)value );
		
		if ( value instanceof Boolean )
			return encode( (Boolean)value );
		
		if ( value instanceof Integer ) 
			return encode( ((Integer)value).intValue() );
		
		if ( value instanceof Long )
			return encode( ((Long)value).longValue() );
		
		if ( value instanceof Character )
			return encode( (Character)value );
		
		if ( value instanceof String )
			return encode( (String)value );
		
		if ( value instanceof StringBuffer )
			return encode( (StringBuffer)value );
		
		if ( value instanceof Float )
			return encode( ((Float)value).floatValue() );
		
		if ( value instanceof Short )
			return encode( (Short)value );
		
		if ( value instanceof Double )
			return encode( ((Double)value).doubleValue() );
		
		if ( value instanceof Date )
			return encode( (Date)value);
		
		if ( value instanceof StringBuilder )
			return encode( (StringBuilder)value );
		
		if ( value instanceof byte[] )
			return encode( (byte[])value );
		
		return null;
	}

	private final static byte[] encode( Byte value ) {
		byte[] b = new byte[1];
		b[0] = value.byteValue();
		return b;
	}

	private final static byte[] encode( Boolean value ) {
		byte[] b = new byte[1];

		if ( value.booleanValue() )
			b[0] = 1;
		else
			b[0] = 0;

		return b;
	}

	private final static byte[] encode( int value ) {
		return getBytes( value );
	}
	
	private final static byte[] encode( long value ) throws Exception {
		return getBytes( value );
	}
	
	private final static byte[] encode( Date value ) {
		return getBytes( value.getTime() );
	}
	
	private final static byte[] encode( Character value ) {
		return encode( value.charValue() );
	}
	
	private final static byte[] encode( String value ) throws Exception {
		return value.getBytes( "UTF-8" );
	}
	
	private final static byte[] encode( StringBuffer value ) throws Exception {
		return encode( value.toString() );
	}
	
	private final static byte[] encode( float value ) throws Exception {
		return encode( (int)Float.floatToIntBits( value ) );
	}
	
	private final static byte[] encode( Short value ) throws Exception {
		return encode( (int)value.shortValue() );
	}
	
	private final static byte[] encode( double value ) throws Exception {
		return encode( (long)Double.doubleToLongBits( value ) );
	}
	
	private final static byte[] encode( StringBuilder value ) throws Exception {
		return encode( value.toString() );
	}
	
	private final static byte[] encode( byte[] value ) {
		return value;
	}

	public final static byte[] getBytes( long value ) {
		byte[] b = new byte[8];
		b[0] = (byte)((value >> 56) & 0xFF);
		b[1] = (byte)((value >> 48) & 0xFF);
		b[2] = (byte)((value >> 40) & 0xFF);
		b[3] = (byte)((value >> 32) & 0xFF);
		b[4] = (byte)((value >> 24) & 0xFF);
		b[5] = (byte)((value >> 16) & 0xFF);
		b[6] = (byte)((value >> 8) & 0xFF);
		b[7] = (byte)((value >> 0) & 0xFF);
		return b;
	}
	
	public final static byte[] getBytes( int value ) {
		byte[] b = new byte[4];
		b[0] = (byte)((value >> 24) & 0xFF);
		b[1] = (byte)((value >> 16) & 0xFF);
		b[2] = (byte)((value >> 8) & 0xFF);
		b[3] = (byte)((value >> 0) & 0xFF);
		return b;
	}
	
	
	
	// decode methods
	public final static Byte decodeByte( byte[] b ) {
		return new Byte( b[0] );
	}
	
	public final static Boolean decodeBoolean( byte[] b ) {
		boolean value = b[0] == 1;
		return ( value ) ? Boolean.TRUE : Boolean.FALSE;
	}
	
	public final static Integer decodeInteger( byte[] b ) {
		return new Integer( toInt( b ) );
	}
	
	public final static Long decodeLong( byte[] b ) throws Exception {
		return new Long( toLong( b ) );
	}
	
	public final static Character decodeCharacter( byte[] b ) {
		return new Character( (char)decodeInteger( b ).intValue() );
	}
	
	public final static String decodeString( byte[] b ) throws Exception {
		return new String( b, "UTF-8" );
	}
	
	public final static StringBuffer decodeStringBuffer( byte[] b ) throws Exception {
		return new StringBuffer( decodeString( b ) );
	}
	
	public final static Float decodeFloat( byte[] b ) throws Exception {
		Integer l = decodeInteger( b );
		return new Float( Float.intBitsToFloat( l.intValue() ) );
	}
	
	public final static Short decodeShort( byte[] b ) throws Exception {
		return new Short( (short)decodeInteger( b ).intValue() );
	}
	
	public final static Double decodeDouble( byte[] b ) throws Exception {
		Long l = decodeLong( b );
		return new Double( Double.longBitsToDouble( l.longValue() ) );
	}
	
	public final static Date decodeDate( byte[] b ) {
		return new Date( toLong( b ) );
	}
	
	public final static StringBuilder decodeStringBuilder( byte[] b ) throws Exception {
		return new StringBuilder( decodeString( b ) );
	}
	
	public final static byte[] decodeByteArr( byte[] b ) {
		return b;
	}
	
	public final static int toInt( byte[] b ) {
		return (((((int) b[3]) & 0xFF) << 32) +
			((((int) b[2]) & 0xFF) << 40) +
			((((int) b[1]) & 0xFF) << 48) +
			((((int) b[0]) & 0xFF) << 56));
	}    
	
	public final static long toLong( byte[] b ) {
		return ((((long) b[7]) & 0xFF) +
			((((long) b[6]) & 0xFF) << 8) +
			((((long) b[5]) & 0xFF) << 16) +
			((((long) b[4]) & 0xFF) << 24) +
			((((long) b[3]) & 0xFF) << 32) +
			((((long) b[2]) & 0xFF) << 40) +
			((((long) b[1]) & 0xFF) << 48) +
			((((long) b[0]) & 0xFF) << 56));
	}    

	
	/**
	 * バイト配列を16進数の文字列に変換する。
	 * 
	 * @param bytes バイト配列
	 * @return 16進数の文字列
	 */
	public final static String toHexString(byte bytes[]) {
		StringBuffer strbuf = new StringBuffer(bytes.length * 2);

		for (int index = 0; index < bytes.length; index++) {
			int bt = bytes[index] & 0xff;
			if (bt < 0x10) {
				strbuf.append("0");
			}
			strbuf.append(Integer.toHexString(bt));
		}
		return strbuf.toString();
	}

	/**
	 * 16進数の文字列をバイト配列に変換する。
	 * 
	 * @param hex 16進数の文字列
	 * @return バイト配列
	 */
	public final static byte[] toByteArray(String hexstring) {
		byte[] bytes = new byte[hexstring.length() / 2];

		for (int index = 0; index < bytes.length; index++) {
			bytes[index] =
				(byte) Integer.parseInt(
						hexstring.substring(index * 2, (index + 1) * 2),
					16);
		}
		return bytes;
	}
	
	public final static boolean isValidCompressionType(String compression_type){
		if(compression_type==null){
			return false;
		}
		for(int i=0;i<CONFIG_VALUES_COMPRESSION_TYPES.length;i++){
			if(compression_type.equalsIgnoreCase(CONFIG_VALUES_COMPRESSION_TYPES[i])){
				return true;
			}
		}
		return false;
	}
	
	public final static int getCompressionTypeCode(String compression_type){
		if(compression_type!=null){
			for(int i=0;i<CONFIG_VALUES_COMPRESSION_TYPES.length;i++){
				if(compression_type.equalsIgnoreCase(CONFIG_VALUES_COMPRESSION_TYPES[i])){
					return i;
				}
			}
		}
		return 0;
	}

	private final static DeflaterOutputStream getCompressior(ByteArrayOutputStream out,int compression_type) {
		switch(compression_type){
		case CODE_COMPRESSION_TYPE_DEFLATE:
			return new DeflaterOutputStream(out);
		case CODE_COMPRESSION_TYPE_GZIP:
			try {
				return new GZIPOutputStream(out);
			} catch (IOException e) {
				logger.error("Error on creating GZIPInputStream",e);
			}
		}
		return null;
	}
	private final static InflaterInputStream getExtractor(ByteArrayInputStream in,int compression_type) {
		switch(compression_type){
		case CODE_COMPRESSION_TYPE_DEFLATE:
			return new InflaterInputStream(in);
		case CODE_COMPRESSION_TYPE_GZIP:
			try {
				return new GZIPInputStream(in);
			} catch (IOException e) {
				logger.error("Error on creating GZIPInputStream",e);
			}
		}
		return null;
	}
	
	public final static String compress(String value,int compression_type) throws Exception{
		
		ByteArrayOutputStream out = null;
		OutputStream output = null;
		try {
			out = new ByteArrayOutputStream();
			output = getCompressior(out,compression_type);
			if(output==null){
				throw new Exception("Failed to get compressor["+compression_type+"].");
			}
			output.write(value.getBytes());
			output.close();
			byte[] b = out.toByteArray();
			return toHexString(b);
		}catch(Exception e){
			e.printStackTrace();
			throw e;
		}finally{
			if(output!=null){
				output.close();
			}
			if(out!=null){
				out.close();
			}
		}
	}
	
	public final static String extract(String value,int compression_type) throws Exception{

		ByteArrayOutputStream out = null;
		ByteArrayInputStream in = null;
		InputStream input = null;
		try {
			in = new ByteArrayInputStream(toByteArray(value));
			input =getExtractor(in,compression_type);
			if(input==null){
				throw new Exception("Failed to get extractor["+compression_type+"].");
			}
			out = new ByteArrayOutputStream();
			int i;
			while ((i = input.read()) != -1) {
				out.write(i);
			}
			return new String(out.toByteArray());
		}finally{
			if(input!=null){
				input.close();
			}
			if(in!=null){
				in.close();
			}
			if(out!=null){
				out.close();
			}
		}
	}
}
