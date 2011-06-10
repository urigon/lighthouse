package jp.co.fujisan.lighthouse.client;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.URLEncoder;
import java.net.UnknownHostException;
//import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import jp.co.fujisan.lighthouse.CodingUtils;
import jp.co.fujisan.lighthouse.client.exception.UnRecoverableException;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MemcachedClient extends KVSClient {
	protected Log logger = LogFactory.getLog(MemcachedClient.class);
	
	// return codes
	protected static final String VALUE        = "VALUE";			// start of value line from server
	protected static final String STATS        = "STAT";			// start of stats line from server
	protected static final String ITEM         = "ITEM";			// start of item line from server
	protected static final String DELETED      = "DELETED";		// successful deletion
	protected static final String NOTFOUND     = "NOT_FOUND";		// record not found for delete or incr/decr
	protected static final String STORED       = "STORED";		// successful store of data
	protected static final String NOTSTORED    = "NOT_STORED";	// data not stored
	protected static final String OK           = "OK";			// success
	protected static final String END          = "END";			// end of data from server

	protected static final String STATS_KEY_ITEMS          = "items";			
	protected static final String STATS_KEY_CACHEDUMP          = "cachedump";
	
	protected static final String ERROR        = "ERROR";			// invalid command name from client
	protected static final String CLIENT_ERROR = "CLIENT_ERROR";	// client error in input line - invalid protocol
	protected static final String SERVER_ERROR = "SERVER_ERROR";	// server error

	protected static final byte[] B_END        = "END\r\n".getBytes();
	protected static final byte[] B_NOTFOUND   = "NOT_FOUND\r\n".getBytes();
	protected static final byte[] B_DELETED    = "DELETED\r\r".getBytes();
	protected static final byte[] B_STORED     = "STORED\r\r".getBytes();

	// default compression threshold
	protected static final int COMPRESS_THRESH = 30720;
    
	// values for cache flags 
	protected static final int MARKER_BYTE             = 1;
	protected static final int MARKER_BOOLEAN          = 8192;
	protected static final int MARKER_INTEGER          = 4;
	protected static final int MARKER_LONG             = 16384;
	protected static final int MARKER_CHARACTER        = 16;
	protected static final int MARKER_STRING           = 32;
	protected static final int MARKER_STRINGBUFFER     = 64;
	protected static final int MARKER_FLOAT            = 128;
	protected static final int MARKER_SHORT            = 256;
	protected static final int MARKER_DOUBLE           = 512;
	protected static final int MARKER_DATE             = 1024;
	protected static final int MARKER_STRINGBUILDER    = 2048;
	protected static final int MARKER_BYTEARR          = 4096;
	protected static final int F_COMPRESSED            = 2;
	protected static final int F_SERIALIZED            = 8;
	
	protected static final String DEFAULT_ENCODING= "UTF-8";

	// flags
	private boolean sanitizeKeys = false;
	private boolean primitiveAsString = false;
	private boolean compressEnable = false;
	private long compressThreshold = COMPRESS_THRESH;
	private String defaultEncoding = DEFAULT_ENCODING;
	
	public final static String CONFIG_FILE_NAME = "memcached.properties";

	// optional passed in classloader
	private ClassLoader classLoader;
	
	public MemcachedClient(String host,int host_port) throws Exception {
		super(host,host_port);
		super.className = MemcachedClient.class.getSimpleName();
		this.sanitizeKeys       = true;
		this.primitiveAsString  = true;
		this.compressEnable     = true;
		this.compressThreshold  = COMPRESS_THRESH;
		this.defaultEncoding    = "UTF-8";
	}

	/**
	 * @param name
	 * @param id
	 * @param host
	 * @throws UnknownHostException 
	 */
	public MemcachedClient(String name, Integer id, int weight,String host,int host_port, Map<String,Object> context) throws Exception {
		super(name,id,weight,host,host_port,context);
		super.className = MemcachedClient.class.getSimpleName();

		this.sanitizeKeys       = true;
		this.primitiveAsString  = true;
		this.compressEnable     = true;
		this.compressThreshold  = COMPRESS_THRESH;
		this.defaultEncoding    = "UTF-8";
		/*
		try{
			File configFile = new File(LightHouse.getHome()+CONFIG_FILE_NAME);
			config =  new PropertiesConfiguration(configFile);
			config.setAutoSave(true);
			
			LightHouse cloud  = LightHouse.getInstance();
			String key = "sanitize_keys";
			  try{
				  if(config.containsKey(key)){
					  sanitizeKeys = config.getBoolean(key); 
				  }else{
					  throw new Exception(key + " is null.");
				  }
			  }catch(Exception e){ 
				  sanitizeKeys = cloud.get_cgf_sanitize_keys(); 
				  config.setProperty(key, sanitizeKeys);
			  }

			  key = "store_primitives_as_string";
			  try{
				  if(config.containsKey(key)){
					  primitiveAsString = config.getBoolean(key); 
				  }else{
					  throw new Exception(key + " is null.");
				  }
			  }catch(Exception e){ 
				  primitiveAsString = cloud.get_cgf_store_primitives_as_string();
				  config.setProperty(key, primitiveAsString);
			  }

			  key = "compress";
			  try{
				  if(config.containsKey(key)){
					  compressEnable = config.getBoolean(key); 
				  }else{
					  throw new Exception(key + " is null.");
				  }
			  }catch(Exception e){ 
				  compressEnable = false;
				  config.setProperty(key, compressEnable);
			  }

			  key = "compress_threshold";
			  try{
				  if(config.containsKey(key)){
					  compressThreshold = config.getLong(key); 
				  }else{
					  throw new Exception(key + " is null.");
				  }
			  }catch(Exception e){ 
				  compressThreshold = MemcachedClient.COMPRESS_THRESH;
				  config.setProperty(key, compressThreshold);
			  }

			  key = "default_encoding";
			  try{
				  if(config.containsKey(key)){
					  defaultEncoding = config.getString(key); 
				  }else{
					  throw new Exception(key + " is null.");
				  }
				  if(defaultEncoding == null||defaultEncoding.length()==0){
					  throw new Exception(key + " is null.");
				  }
			  }catch(Exception e){ 
				  defaultEncoding = MemcachedClient.DEFAULT_ENCODING;
				  config.setProperty(key, defaultEncoding);
			  }
			  config.save();
			
		}catch(Exception e){
			e.printStackTrace();
		}*/
		try{
			this.classLoader = this.getClass().getClassLoader();
		}catch(Exception ignore){
			
		}
	}
	
	/** 
	 * Stores data to cache.
	 *
	 * If data does not already exist for this key on the server, or if the key is being<br/>
	 * deleted, the specified value will not be stored.<br/>
	 * The server will automatically delete the value when the expiration time has been reached.<br/>
	 * <br/>
	 * If compression is enabled, and the data is longer than the compression threshold<br/>
	 * the data will be stored in compressed form.<br/>
	 * <br/>
	 * As of the current release, all objects stored will use java serialization.
	 * 
	 * @param cmdname action to take (set, add, replace)
	 * @param key key to store cache under
	 * @param value object to cache
	 * @return true/false indicating success
	 */
	
	final public boolean set( String key, Object value)throws Exception {
		if(!this.isAvailable){
			return false;
		}

		boolean result = false; 
		
		if ( value == null ) {
			throw new Exception("trying to store a null value to cache");
		}

		/*
		 * TODO 有効期間の設定もnodes.xmlから取得可能にする。
		 **/
		Date expiry = new Date(0);
		
		if ( expiry == null )
			expiry = new Date(0);

		// store flags
		int flags = 0;
		
		// byte array to hold data
		byte[] val;

        if ( CodingUtils.isHandled( value ) ) {
			
			if ( this.primitiveAsString ) {
				// useful for sharing data between java and non-java
				// and also for storing ints for the increment method
				//logger.debug( "++++ storing data as a string for key: " + key + " for class: " + value.getClass().getName() );
				val = value.toString().getBytes( defaultEncoding );
			}
			else {
				//logger.debug( "Storing with native handler..." );
				flags |= getMarkerFlag( value );
				val    = CodingUtils.encode( value );
			}
		}
		else {
			// always serialize for non-primitive types
			//logger.debug( "++++ serializing for key: " + key + " for class: " + value.getClass().getName() );
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			(new ObjectOutputStream( bos )).writeObject( value );
			val = bos.toByteArray();
			flags |= F_SERIALIZED;
		}
		
		// now try to compress if we want to
		// and if the length is over the threshold 
		if ( compressEnable && val.length > compressThreshold ) {

			//logger.debug( "++++ trying to compress data" );
			//logger.debug( "++++ size prior to compression: " + val.length );
			ByteArrayOutputStream bos = new ByteArrayOutputStream( val.length );
			GZIPOutputStream gos = new GZIPOutputStream( bos );
			gos.write( val, 0, val.length );
			gos.finish();
			
			// store it and set compression flag
			val = bos.toByteArray();
			flags |= F_COMPRESSED;

			//logger.debug( "++++ compression succeeded, size after: " + val.length );
		}

		// now write the data to the cache server
		String cmd = String.format( "%s %s %d %d %d\r\n", "set", sanitizeKey( key ), flags, (expiry.getTime() / 1000), val.length );
		
		try {
			
			//Execute SET command on host server.
			result = executeSet(cmd,val);
			
		} catch (Exception e) {
			this.onFail(e);
		}finally{
			//disconnect();
		}

		return result;
	}
	
	private boolean executeSet(String cmd,byte[] val) throws Exception{
		Socket socket = getSocket(); 
		try {
			OutputStream out = new BufferedOutputStream( socket.getOutputStream());
			out.write( cmd.getBytes() );
			out.write( val );
			out.write( "\r\n".getBytes() );
			out.flush();
			
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			String ret = in.readLine();
			if(ret==null){
				return false;
			}

			if ( STORED.equals( ret ) ) {
				return true;
			}else if ( ret.startsWith(SERVER_ERROR) ) {
				throw new UnRecoverableException(ret);
			}
			
			
		} catch (Exception e) {
			throw e;
		}finally{
			releaseSocket(socket);
		}
		return false;
	}
	
	/**
	 * Retrieve a key from the server, using a specific hash.
	 *
	 *  If the data was compressed or serialized when compressed, it will automatically<br/>
	 *  be decompressed or serialized, as appropriate. (Inclusive or)<br/>
	 *<br/>
	 *  Non-serialized data will be returned as a string, so explicit conversion to<br/>
	 *  numeric types will be necessary, if desired<br/>
	 *
	 * @param key key where data is stored
	 * @return the object that was previously stored, or null if it was not previously stored
	 */

	final public Object get( String key )throws Exception {
		if(!this.isAvailable){
			return null;
		}
		// ready object
		Object result = null;

		if ( key == null ) {
			throw new Exception( "key is null for get()" );
		}

		// now write the data to the cache server
		String cmd = "get " + sanitizeKey( key ) ;
		
		try {

			//Execute SET command on host server.
			result = executeGet(cmd);
			
		} catch (Exception e) {
			this.onFail(e);
		}
		
		return result;
	}

	@Override
	final public boolean delete(String key) throws Exception {
		if(!this.isAvailable){
			return false;
		}
		// ready object
		Object result = null;

		if ( key == null ) {
			throw new Exception( "key is null for get()" );
		}

		// now write the data to the cache server
		String cmd = "delete " + sanitizeKey( key ) ;
		
		try {
			
			//Execute SET command on host server.
			result = executeGet(cmd);
			
		} catch (Exception e) {
			this.onFail(e);
		}
		
		if(result==null){
			result = false;
		}
		return (Boolean)result;
	}
	
	private Object executeGet(String cmd)throws Exception{
		Object result = null;
		Socket socket = getSocket(); 
		try {
			
			OutputStream out = new BufferedOutputStream( socket.getOutputStream());
			out.write( cmd.getBytes() );
			out.write( "\r\n".getBytes() );
			out.flush();
			
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			while ( true ) {
				String line = in.readLine();
				if(line==null){
					result = false;
					break;
				}
				if ( END.equals( line ) ) {
					break;
				}else if ( DELETED.equals( line ) ) {
					result = true;
					break;
				}else if ( NOTFOUND.equals( line ) ) {
					result = false;
					break;
				}else if ( line.startsWith(SERVER_ERROR) ) {
					throw new UnRecoverableException(line);
				}else	if ( line.startsWith( VALUE ) ) {
					String[] info = line.split(" ");
					int flag      = Integer.parseInt( info[2] );
					int length    = Integer.parseInt( info[3] );

					if(length>0){

						 line = in.readLine();
						 byte[] buf =line.getBytes();

						if ( (flag & F_COMPRESSED) == F_COMPRESSED ) {
							try {
								// read the input stream, and write to a byte array output stream since
								// we have to read into a byte array, but we don't know how large it
								// will need to be, and we don't want to resize it a bunch
								GZIPInputStream gzi = new GZIPInputStream( new ByteArrayInputStream( buf ) );
								ByteArrayOutputStream bos = new ByteArrayOutputStream( buf.length );
								
								int count;
								byte[] tmp = new byte[2048];
								while ( (count = gzi.read(tmp)) != -1 ) {
									bos.write( tmp, 0, count );
								}

								// store uncompressed back to buffer
								buf = bos.toByteArray();
								gzi.close();
							}
							catch ( IOException e ) {
								throw new Exception( "++++ IOException thrown while trying to uncompress input stream for  cmd: " + cmd , e );
							}
						}

						// we can only take out serialized objects
						if ( ( flag & F_SERIALIZED ) != F_SERIALIZED ) {
							if ( primitiveAsString ) {
								// pulling out string value
								result = new String( buf, defaultEncoding );
							}
							else {
								// decoding object
								try {
									result = decode( buf, flag );    
								}
								catch ( Exception e ) {
									throw new Exception( e );
								}
							}
						}
						else if(classLoader != null){
							// deserialize if the data is serialized
							ContextObjectInputStream ois =
								new ContextObjectInputStream( new ByteArrayInputStream( buf ), classLoader );
							try {
								result = ois.readObject();
							}
							catch ( ClassNotFoundException e ) {
								throw new Exception( "+++ failed while trying to deserialize for cmd: " + cmd, e );
							}
						}else{
							return "";
						}
					}else{
						return null;
					}
				}
			}
			
		} catch (Exception e) {
			throw e;
		}finally{
			releaseSocket(socket); 
		}
		return result;
	}
	
	/**
	 * Retrieve a list of keys from the server, using a specific hash.
	 *
	 * @return the object that was previously stored, or null if it was not previously stored
	 */
	@Override
	final public Set<String> keys( )throws Exception {

		// ready object
		Set<String> result = new HashSet<String>();

				// now write the data to the cache server
		String cmd = "stats ";
		String key =STATS_KEY_ITEMS;
		
		try {
			//connect();
			
			Map<String,Object> stats = new HashMap<String,Object>();
			
			//Execute SET command on host server.
			stats = executeStats(cmd,key,null);
			
			if(stats!=null&&!stats.isEmpty()){
				Iterator<String> ite = stats.keySet().iterator();
				while(ite.hasNext()){
					String slab = ite.next();
					Integer count = Integer.parseInt((String)stats.get(slab));
					
					//Limit entry item number
					if(count>10000){
						count = 10000;
					}
					key = STATS_KEY_CACHEDUMP;
					String options = " " + slab + " " + count;

					Map<String,Object> stats2 = new HashMap<String,Object>();
					
					//Execute SET command on host server.
					stats2 = executeStats(cmd,key,options);
					
					if(stats2!=null&&!stats2.isEmpty()){
						Iterator<String> ite2 = stats2.keySet().iterator();
						while(ite2.hasNext()){
							String item_key = desanitizeKey(ite2.next());
							result.add(item_key);
						}
					}
				}
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			this.onFail(e);
		}finally{
			//disconnect();
		}
		
		return result;

	}
	
	@Override
	final public int size() {
		// ready object
		Integer result = 0;

		// now write the data to the cache server
		String cmd = "stats ";
		String key =STATS_KEY_ITEMS;
		
		try {
			//connect();
			
			Map<String,Object> stats = new HashMap<String,Object>();
			
			//Execute SET command on host server.
			stats = executeStats(cmd,key,null);
			
			if(stats!=null&&!stats.isEmpty()){
				Iterator<String> ite = stats.keySet().iterator();
				while(ite.hasNext()){
					String slab = ite.next();
					result += Integer.parseInt((String)stats.get(slab));
				}
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			this.onFail(e);
		}finally{
			//disconnect();
		}
		
		return result;
		
	}
	
	private synchronized Map<String,Object> executeStats(String cmd,String key,String options)throws Exception{
		
		Map<String,Object> stats = null;
		
		Socket socket = getSocket();
		try {
			
			if(socket!=null&&socket.isConnected()){
				OutputStream out = new BufferedOutputStream( socket.getOutputStream());
				out.write( cmd.getBytes() );
				out.write( key.getBytes() );
				if(options!=null&&options.length()>0){
					out.write( options.getBytes() );
				}
				out.write( "\r\n".getBytes() );
				out.flush();
				
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				stats = new HashMap<String,Object>();
				while ( true ) {
					String line = in.readLine();
					if(line==null){
						break;
					}
					//logger.debug( "++++ line: " + line );
					if ( END.equals( line ) ) {
						//logger.debug( "++++ finished reading from cache server" );
						break;
					}

					if(STATS_KEY_ITEMS.compareTo(key)==0){
						if ( line.startsWith( STATS ) ) {
							Pattern p = Pattern.compile("^STAT\\sitems:(\\d+):number\\s(\\d+)$");
							 Matcher m = p.matcher(line);
							 if (m.find()) {
							     String slab = m.group(1); //slab number
							     String count = m.group(2); //item count
							     stats.put(slab, count);
							     //System.out.println("slab   = " + slab + " count = " + count);
							 }		
							 continue;
						}
					}
					if(STATS_KEY_CACHEDUMP.compareTo(key)==0){
						if ( line.startsWith( ITEM ) ) {
							Pattern p = Pattern.compile("^ITEM\\s(.+)\\s\\[(.+)\\]$");
							 Matcher m = p.matcher(line);
							 if (m.find()) {
							     String item_key = m.group(1); //item key
							     String meta = m.group(2); //[$value_length b; $expiration s]
							     stats.put(item_key, null);
							     //System.out.println("key   = " + item_key + " " + meta);
							 }		
							 continue;
						}
					}
					
				}
			}
			
		} catch (Exception e) {
			throw e;
		}finally{
			releaseSocket(socket);
		}
		return stats;
	}
	
	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	final public void terminate(){
		classLoader = null;
		super.terminate();
	}
		
	@Override
	final public void onFail(Exception e){
		classLoader = null;
		super.onFail(e);
	}
	
	@Override
	final public String getType() {
		// TODO Auto-generated method stub
		return ClientFactory.SERVER_TYPE_MEMCACHED;
	}
	
	public static int getMarkerFlag( Object value ) {

		if ( value instanceof Byte )
			return MARKER_BYTE;
		
		if ( value instanceof Boolean )
			return MARKER_BOOLEAN;
		
		if ( value instanceof Integer ) 
			return MARKER_INTEGER;
		
		if ( value instanceof Long )
			return MARKER_LONG;

		if ( value instanceof Character )
			return MARKER_CHARACTER;
		
		if ( value instanceof String )
			return MARKER_STRING;
		
		if ( value instanceof StringBuffer )
			return MARKER_STRINGBUFFER;
		
		if ( value instanceof Float )
			return MARKER_FLOAT;
		
		if ( value instanceof Short )
			return MARKER_SHORT;
		
		if ( value instanceof Double )
			return MARKER_DOUBLE;
		
		if ( value instanceof Date )
			return MARKER_DATE;
		
		if ( value instanceof StringBuilder )
			return MARKER_STRINGBUILDER;
		
		if ( value instanceof byte[] )
			return MARKER_BYTEARR;
		
		return -1;
	}
	public static Object decode( byte[] b, int flag ) throws Exception {

		if ( b.length < 1 )
			return null;

		
		if ( ( flag & MARKER_BYTE ) == MARKER_BYTE )
			return CodingUtils.decodeByte( b );
		
		if ( ( flag & MARKER_BOOLEAN ) == MARKER_BOOLEAN )
			return CodingUtils.decodeBoolean( b );
		
		if ( ( flag & MARKER_INTEGER ) == MARKER_INTEGER )
			return CodingUtils.decodeInteger( b );
		
		if ( ( flag & MARKER_LONG ) == MARKER_LONG )
			return CodingUtils.decodeLong( b );
		
		if ( ( flag & MARKER_CHARACTER ) == MARKER_CHARACTER )
			return CodingUtils.decodeCharacter( b );
		
		if ( ( flag & MARKER_STRING ) == MARKER_STRING )
			return CodingUtils.decodeString( b );
		
		if ( ( flag & MARKER_STRINGBUFFER ) == MARKER_STRINGBUFFER )
			return CodingUtils.decodeStringBuffer( b );
		
		if ( ( flag & MARKER_FLOAT ) == MARKER_FLOAT )
			return CodingUtils.decodeFloat( b );
		
		if ( ( flag & MARKER_SHORT ) == MARKER_SHORT )
			return CodingUtils.decodeShort( b );
		
		if ( ( flag & MARKER_DOUBLE ) == MARKER_DOUBLE )
			return CodingUtils.decodeDouble( b );
		
		if ( ( flag & MARKER_DATE ) == MARKER_DATE )
			return CodingUtils.decodeDate( b );
		
		if ( ( flag & MARKER_STRINGBUILDER ) == MARKER_STRINGBUILDER )
			return CodingUtils.decodeStringBuilder( b );
		
		if ( ( flag & MARKER_BYTEARR ) == MARKER_BYTEARR )
			return CodingUtils.decodeByteArr( b );
		
		return null;
	}

}
