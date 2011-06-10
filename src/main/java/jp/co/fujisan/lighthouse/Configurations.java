package jp.co.fujisan.lighthouse;

import java.io.File;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jp.co.fujisan.lighthouse.exception.StatusException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.event.ConfigurationEvent;
import org.apache.commons.configuration.event.ConfigurationListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

public class Configurations implements Serializable{

	private static final Log logger = LogFactory.getLog(Configurations.class);

	public static final Integer VERSION = 1;
	
	/*
	 * Configuration Parameters
	 * */
	public final static String CONFIG_KEY_VERSION = "VERSION";
	public final static String CONFIG_KEY_RING_ID = "ring_id";
	public final static String CONFIG_KEY_DEBUG = "debug_on";
	public final static String CONFIG_KEY_FORCE_SHUTDOWN = "force_shutdown";
	public final static String CONFIG_KEY_SANITIZE_KEY = "sanitize_keys";
	public final static String CONFIG_KEY_STORE_PRIMITIVES_AS_STRING = "store_primitives_as_string";
	public final static String CONFIG_KEY_SANITIZE_ENCODING = "sanitize_encoding";
	public final static String CONFIG_KEY_REPLICA_NUMBER = "replica_number";
	public final static String CONFIG_KEY_GET_RETRY_NUMBER = "get_retry_number";	
	public final static String CONFIG_KEY_ASYNC_THREAD_NUMBER = "async_thread_number";	
	public final static String CONFIG_KEY_COMMIT_ASYNC = "commit_async";	
	public final static String CONFIG_KEY_CHECK_CLIENT_AVAILABILITY_STRICTLY = "check_client_availability_strictly";	
	public final static String CONFIG_KEY_GET_READ_LOCK_TIMEOUT_MILLIS = "get_read_lock_timeout_millis";	
	public final static String CONFIG_KEY_QUEUE_SIZE_LIMIT = "queue_size_limit";	
	public final static String CONFIG_KEY_ENQUEUE_TIMEOUT_MILLIS = "enqueue_timeout_millis";	
	public final static String CONFIG_KEY_COMPRESS_VALUES = "compress_values";	
	public final static String CONFIG_KEY_COMPRESSION_TYPE = "compression_type";	
	public final static String CONFIG_KEY_COMPRESSION_THRESHOLD = "compression_threshold";	
	public final static String CONFIG_KEY_CAS_ENABLE = "cas_enable";	
	public final static String CONFIG_KEY_LOCK_GROUP = "lock_group";	
	public final static String CONFIG_KEY_GLOBAL_LOCK_ENABLE = "global_lock_enable";	
	public final static String CONFIG_KEY_GLOBAL_LOCK_TIMEOUT_MILLIS = "global_lock_timeout_millis";	
	public final static String CONFIG_KEY_GLOBAL_LOCK_LOCAL_BIND = "global_lock_local_bind";	
	public final static String CONFIG_KEY_GLOBAL_LOCK_REMOTE_HOSTS = "global_lock_remote_hosts";	
	public final static String CONFIG_KEY_IGNORE_CLIENT_FAILURE = "ignore_client_failure";
	public final static String CONFIG_KEY_CONSOLE_ENABLE = "console_enable";

	public final static boolean CONFIG_DEFAULT_DEBUG = false;
	public final static String CONFIG_DEFAULT_RING_ID = null;
	public final static boolean CONFIG_DEFAULT_FORCE_SHUTDOWN  = false;
	public final static boolean CONFIG_DEFAULT_SANITIZE_KEY = true;
	public final static boolean CONFIG_DEFAULT_STORE_PRIMITIVES_AS_STRING = true;
	public final static String CONFIG_DEFAULT_SANITIZE_ENCODING = "UTF-8";
	public final static int CONFIG_DEFAULT_REPLICA_NUMBER = 0;
	public final static int CONFIG_DEFAULT_GET_RETRY_NUMBER = 0;	
	public final static int CONFIG_DEFAULT_ASYNC_THREAD_NUMBER = 3;	
	public final static boolean CONFIG_DEFAULT_COMMIT_ASYNC = false;	
	public final static boolean CONFIG_DEFAULT_CHECK_CLIENT_AVAILABILITY_STRICTLY = false;	
	public final static int CONFIG_DEFAULT_GET_READ_LOCK_TIMEOUT_MILLIS = 0;	
	public final static int CONFIG_DEFAULT_QUEUE_SIZE_LIMIT = 0;	
	public final static long CONFIG_DEFAULT_ENQUEUE_TIMEOUT_MILLIS = 0;	
	public final static boolean CONFIG_DEFAULT_COMPRESS_VALUES = false;	
	public final static String[] CONFIG_COMPRESSION_TYPES = CodingUtils.CONFIG_VALUES_COMPRESSION_TYPES;
	public final static String CONFIG_DEFAULT_COMPRESSION_TYPE = CONFIG_COMPRESSION_TYPES[CodingUtils.CODE_COMPRESSION_TYPE_DEFLATE];
	public final static int CONFIG_DEFAULT_COMPRESSION_THRESHOLD = 100;	
	public final static boolean CONFIG_DEFAULT_CAS_ENABLE = false;	
	public final static boolean CONFIG_DEFAULT_LOCK_GROUP = false;	
	public final static boolean CONFIG_DEFAULT_GLOBAL_LOCK_ENABLE = false;	
	public final static long CONFIG_DEFAULT_GLOBAL_LOCK_TIMEOUT_MILLIS = 0;	
	public final static String CONFIG_DEFAULT_GLOBAL_LOCK_LOCAL_BIND = "localhost:1976";	
	public final static String CONFIG_DEFAULT_GLOBAL_LOCK_REMOTE_HOSTS = "";	
	public final static boolean CONFIG_DEFAULT_IGNORE_CLIENT_FAILURE = true;	
	public final static boolean CONFIG_DEFAULT_CONSOLE_ENABLE = true;	

	protected boolean debug_on = CONFIG_DEFAULT_DEBUG;
	protected String ring_id = CONFIG_DEFAULT_RING_ID;
	protected boolean force_shutdown = CONFIG_DEFAULT_FORCE_SHUTDOWN;
	protected boolean sanitize_keys = CONFIG_DEFAULT_SANITIZE_KEY;
	protected final boolean store_primitives_as_string = CONFIG_DEFAULT_STORE_PRIMITIVES_AS_STRING;
	protected String sanitize_encoding = CONFIG_DEFAULT_SANITIZE_ENCODING;
	protected int replica_number = CONFIG_DEFAULT_REPLICA_NUMBER;
	protected int get_retry_number = CONFIG_DEFAULT_GET_RETRY_NUMBER;	
	protected int async_thread_number = CONFIG_DEFAULT_ASYNC_THREAD_NUMBER;
	protected boolean commit_async = CONFIG_DEFAULT_COMMIT_ASYNC;
	protected boolean check_client_availability_strictly = CONFIG_DEFAULT_CHECK_CLIENT_AVAILABILITY_STRICTLY;
	protected int get_read_lock_timeout_millis = CONFIG_DEFAULT_GET_READ_LOCK_TIMEOUT_MILLIS;
	protected int queue_size_limit = CONFIG_DEFAULT_QUEUE_SIZE_LIMIT;
	protected long enqueue_timeout_millis = CONFIG_DEFAULT_ENQUEUE_TIMEOUT_MILLIS;
	protected boolean compress_values = CONFIG_DEFAULT_COMPRESS_VALUES;
	protected String compression_type = CONFIG_DEFAULT_COMPRESSION_TYPE;
	protected int compression_type_i = CodingUtils.getCompressionTypeCode(CONFIG_DEFAULT_COMPRESSION_TYPE);
	protected int compression_threshold = CONFIG_DEFAULT_COMPRESSION_THRESHOLD ;	
	protected boolean cas_enable = CONFIG_DEFAULT_CAS_ENABLE;
	protected boolean lock_group = CONFIG_DEFAULT_LOCK_GROUP;
	protected boolean global_lock_enable = CONFIG_DEFAULT_GLOBAL_LOCK_ENABLE;
	protected long global_lock_timeout_millis = CONFIG_DEFAULT_GLOBAL_LOCK_TIMEOUT_MILLIS;
	protected String global_lock_local_bind = CONFIG_DEFAULT_GLOBAL_LOCK_LOCAL_BIND;
	protected String global_lock_remote_hosts = CONFIG_DEFAULT_GLOBAL_LOCK_REMOTE_HOSTS;
	protected boolean ignore_client_failure = CONFIG_DEFAULT_IGNORE_CLIENT_FAILURE;	
	protected boolean console_enable = CONFIG_DEFAULT_CONSOLE_ENABLE;	
	

	
	/*
	 * Configuration Files  
	 */
	protected String configuration_path = "./lighthouse.properties";
	protected String storage_path = "./storage.xml";
	protected PropertiesConfiguration m_configuration = null;
	protected XMLConfiguration m_storage_configuration = null;

	public Configurations(){
	}
	
	/**
	 * 設定ファイルのフルパス
	 * @return
	 */
	public String getConfiguration_path() {
		return configuration_path;
	}

	/**
	 * 設定ファイルのフルパス
	 * @param configurationPath
	 * @throws Exception
	 */
	public void setConfiguration_path(String configurationPath) throws Exception {
		
    	logger.info("Loading configuration file ["+configurationPath+"].");
    	
		try {
	    	File configFile = new File(configurationPath);
	    	
			m_configuration = new PropertiesConfiguration(configFile);
			m_configuration.setAutoSave(true);
		} catch (Exception e) {
			logger.error(e);
			throw e;
		}
		configuration_path = configurationPath;
		
	}
	
	/**
	 * 設定ファイルインスタンス
	 * @return
	 */
	public PropertiesConfiguration getConfiguration() {
		return m_configuration;
	}

	/**
	 * ストレージノード定義リストファイルのフルパス
	 * @return
	 */
	public String getStorage_path() {
		return storage_path;
	}
	
	/**
	 * ストレージノード定義リストファイルのフルパス
	 * @param nodesPath
	 * @throws Exception
	 */
	public void setStorage_path(String storagePath) throws Exception {

    	logger.info("Loading storage node definition file ["+storagePath+"].");
    	
		try {
	    	File storageFile = new File(storagePath);
			
	    	m_storage_configuration = new XMLConfiguration(storageFile);
	    	m_storage_configuration.setAutoSave(true);
			
		} catch (Exception e) {
			logger.error(e);
			throw e;
		}
		storage_path = storagePath;
		
	}
	
	/**
	 * ストレージノード定義リスト
	 * @return
	 */
	public XMLConfiguration getStorage_configuration() {
		return m_storage_configuration;
	}

	/**
	 *ファイルに書き込み
	 * @throws ConfigurationException
	 */
	public void save() throws Exception{
		m_configuration.save();
		m_storage_configuration.save();
	}
	
	/**
	 * ファイルからインスタンスプロパティに展開
	 * @throws ConfigurationException
	 * @throws StatusException 
	 */
	protected void setup() throws ConfigurationException, StatusException{
		
			try {
				m_configuration = new PropertiesConfiguration(configuration_path);
				m_storage_configuration = new XMLConfiguration(storage_path);
			} catch (ConfigurationException e) {
				e.printStackTrace();
			}
			
    		/*
    		 * 設定ファイルバージョンの確認
    		 */
    		if(m_configuration.getInteger(CONFIG_KEY_VERSION,0)!=VERSION){
//	       		logger.warn("lighthouse.properties version is unmatched. ");
    		}
    		
    		setDebug_on(m_configuration.getBoolean(CONFIG_KEY_DEBUG,CONFIG_DEFAULT_DEBUG));
    		setRing_id(m_configuration.getString(CONFIG_KEY_RING_ID,CONFIG_DEFAULT_RING_ID));
    		setForce_shutdown(m_configuration.getBoolean(CONFIG_KEY_FORCE_SHUTDOWN,CONFIG_DEFAULT_FORCE_SHUTDOWN));
			setSanitize_keys(m_configuration.getBoolean(CONFIG_KEY_SANITIZE_KEY,CONFIG_DEFAULT_SANITIZE_KEY));
			setSanitize_encoding(m_configuration.getString(CONFIG_KEY_SANITIZE_ENCODING,CONFIG_DEFAULT_SANITIZE_ENCODING));
    		setReplica_number(m_configuration.getInt(CONFIG_KEY_REPLICA_NUMBER,CONFIG_DEFAULT_REPLICA_NUMBER));
    		setGet_retry_number(m_configuration.getInt(CONFIG_KEY_GET_RETRY_NUMBER,CONFIG_DEFAULT_GET_RETRY_NUMBER));
			setCommit_async(m_configuration.getBoolean(CONFIG_KEY_COMMIT_ASYNC,CONFIG_DEFAULT_COMMIT_ASYNC));
			setAsync_thread_number(m_configuration.getInt(CONFIG_KEY_ASYNC_THREAD_NUMBER,CONFIG_DEFAULT_ASYNC_THREAD_NUMBER));
    		setCheck_client_availability_strictly(m_configuration.getBoolean(CONFIG_KEY_CHECK_CLIENT_AVAILABILITY_STRICTLY,CONFIG_DEFAULT_CHECK_CLIENT_AVAILABILITY_STRICTLY));
			setGet_read_lock_timeout_millis(m_configuration.getInt(CONFIG_KEY_GET_READ_LOCK_TIMEOUT_MILLIS,CONFIG_DEFAULT_GET_READ_LOCK_TIMEOUT_MILLIS));
    		setQueue_size_limit(m_configuration.getInt(CONFIG_KEY_QUEUE_SIZE_LIMIT,CONFIG_DEFAULT_QUEUE_SIZE_LIMIT));
    		setEnqueue_timeout_millis(m_configuration.getLong(CONFIG_KEY_ENQUEUE_TIMEOUT_MILLIS,CONFIG_DEFAULT_ENQUEUE_TIMEOUT_MILLIS));
    		setCompress_values(m_configuration.getBoolean(CONFIG_KEY_COMPRESS_VALUES,CONFIG_DEFAULT_COMPRESS_VALUES));
    		setCompression_type(m_configuration.getString(CONFIG_KEY_COMPRESSION_TYPE, CONFIG_DEFAULT_COMPRESSION_TYPE));
    		setCompression_threshold(m_configuration.getInt(CONFIG_KEY_COMPRESSION_THRESHOLD,CONFIG_DEFAULT_COMPRESSION_THRESHOLD));
    		setCas_enable(m_configuration.getBoolean(CONFIG_KEY_CAS_ENABLE,CONFIG_DEFAULT_CAS_ENABLE));
    		setLock_group(m_configuration.getBoolean(CONFIG_KEY_LOCK_GROUP,CONFIG_DEFAULT_LOCK_GROUP));
    		setGlobal_lock_enable(m_configuration.getBoolean(CONFIG_KEY_GLOBAL_LOCK_ENABLE,CONFIG_DEFAULT_GLOBAL_LOCK_ENABLE));
    		setGlobal_lock_local_bind(m_configuration.getString(CONFIG_KEY_GLOBAL_LOCK_LOCAL_BIND,CONFIG_DEFAULT_GLOBAL_LOCK_LOCAL_BIND));
    		setGlobal_lock_remote_hosts(m_configuration.getString(CONFIG_KEY_GLOBAL_LOCK_REMOTE_HOSTS,CONFIG_DEFAULT_GLOBAL_LOCK_REMOTE_HOSTS));
    		setGlobal_lock_timeout_millis(m_configuration.getLong(CONFIG_KEY_GLOBAL_LOCK_TIMEOUT_MILLIS, CONFIG_DEFAULT_GLOBAL_LOCK_TIMEOUT_MILLIS));
    		setIgnore_client_failure(m_configuration.getBoolean(CONFIG_KEY_IGNORE_CLIENT_FAILURE,CONFIG_DEFAULT_IGNORE_CLIENT_FAILURE));
    		setIgnore_client_failure(m_configuration.getBoolean(CONFIG_KEY_CONSOLE_ENABLE,CONFIG_DEFAULT_CONSOLE_ENABLE));
    		
    }

    /**
     * デバッグを有効にするか
     * @return
     */
	public boolean isDebug_on() {
		return debug_on;
	}

	/*
     * デバッグを有効にするか
	 */
	public void setDebug_on(boolean debugOn) {
		debug_on = debugOn;
		m_configuration.setProperty(CONFIG_KEY_DEBUG,debug_on);
	}

	/**
	 * インスタンスのリングID
	 * @return
	 */
	public String getRing_id() {
		return ring_id;
	}

	/**
	 * インスタンスのリングID<br/>
	 * 複数のインスタンスがストレージノードを共有しても、エントリが重複しないようにインスタンス毎にキーにプリフィックスをつけられる。
	 * @param ringId
	 */
	public void setRing_id(String ringId) throws ConfigurationException{
		ring_id = ringId;
		m_configuration.setProperty(CONFIG_KEY_RING_ID,ring_id);
	}

    /**
     * 強制終了を有効にするか
     * @return
     */
	public boolean isForce_shutdown() {
		return force_shutdown;
	}

    /**
     * 強制終了を有効にするか
     * @param debug_on
     */
	public void setForce_shutdown(boolean forceShutdown) {
		force_shutdown = forceShutdown;
		m_configuration.setProperty(CONFIG_KEY_FORCE_SHUTDOWN,force_shutdown);
	}

	/**
	 * SET時のkeyをサニタイズするかどうか？
	 * @return
	 */
	public boolean isSanitize_keys() {
		return sanitize_keys;
	}

	/**
	 * SET時のkeyをサニタイズするかどうか？
	 * @param cfg_sanitize_keys
	 * @throws StatusException 
	 */
	public void setSanitize_keys(boolean sanitizeKeys) throws StatusException {
		if(this.lighthouse!=null&&this.lighthouse.getStatus() != LightHouse.STATUS_STOP){
    		throw new StatusException(this.lighthouse.getStatus(),"LightHouse is running now.");
    	}
		sanitize_keys = sanitizeKeys;
		m_configuration.setProperty(CONFIG_KEY_SANITIZE_KEY,sanitize_keys);
	}

	/**
	 * SET時のkeyをサニタイズする時のエンコーディング
	 * @return
	 */
	public String getSanitize_encoding() {
		return sanitize_encoding;
	}

	/**
	 * SET時のkeyをサニタイズする時のエンコーディング
	 * @param cfg_sanitize_encoding
	 * @throws StatusException 
	 */
	public void setSanitize_encoding(String sanitizeEncoding) throws StatusException {
		if(this.lighthouse!=null&&this.lighthouse.getStatus() != LightHouse.STATUS_STOP){
    		throw new StatusException(this.lighthouse.getStatus(),"LightHouse is running now.");
    	}
		sanitize_encoding = sanitizeEncoding;
		m_configuration.setProperty(CONFIG_KEY_SANITIZE_ENCODING,sanitize_encoding);
	}
	

	/**
	 * SET時のレプリカのコピー数
	 * SETされるエントリーはcfg_replica_number+1の数のノードに格納される
	 * @return
	 */
	public int getReplica_number() {
		return replica_number;
	}

	/**
	 * SET時のレプリカのコピー数
	 * @param cfg_replica_number
	 */
	public void setReplica_number(int replicaNumber) {
		replica_number = replicaNumber;
		m_configuration.setProperty(CONFIG_KEY_REPLICA_NUMBER,replica_number);
		if(this.lighthouse!=null){
			this.lighthouse.update_rt_replica_number();
		}
	}


	/**
	 * GET時の最大試行ノード数
	 * @return
	 */
	public int getGet_retry_number() {
		return get_retry_number;
	}

	/**
	 * GET時の最大試行ノード数
	 * 0はすべてのノードを試行する。
	 * @param cfg_get_retry_number
	 */
	public void setGet_retry_number(int getRetryNumber) {
		get_retry_number = getRetryNumber;
		m_configuration.setProperty(CONFIG_KEY_GET_RETRY_NUMBER,get_retry_number);
		if(this.lighthouse!=null){
			this.lighthouse.update_rt_get_retry_number();
		}
	}
	

	/**
	 * 非同期処理する時の最大スレッド数
	 * @return
	 */
	public int getAsync_thread_number() {
		return async_thread_number;
	}

	/**
	 * 非同期処理する時の最大スレッド数
	 * @param cfg_set_thread_number
	 * @throws Exception
	 */
	public void setAsync_thread_number(int asyncThreadNumber)throws StatusException {
		if(this.lighthouse!=null&&this.lighthouse.getStatus() != LightHouse.STATUS_STOP){
    		throw new StatusException(this.lighthouse.getStatus(),"LightHouse is running now.");
    	}
		async_thread_number = asyncThreadNumber;
		if(async_thread_number<1){
			return;
		}
		m_configuration.setProperty(CONFIG_KEY_ASYNC_THREAD_NUMBER,async_thread_number);
    }

	/**
	 * SET/DELETEを非同期処理するか？
	 * @return
	 */
	public boolean isCommit_async() {
		return commit_async;
	}

	/**
	 * SET/DELETEを非同期処理するか？
	 * @param cfg_commit_async
	 * @throws StatusException
	 */
	public void setCommit_async(boolean commitAsync) throws StatusException {
		if(this.lighthouse!=null&&this.lighthouse.getStatus() != LightHouse.STATUS_STOP){
    		throw new StatusException(this.lighthouse.getStatus(),"LightHouse is running now.");
    	}
		commit_async = commitAsync;
		m_configuration.setProperty(CONFIG_KEY_COMMIT_ASYNC,commit_async);
	}

	/**
	 * KVSの死活チェックを厳密にするか？
	 * @return
	 */
	public boolean isCheck_client_availability_strictly() {
		return check_client_availability_strictly;
	}

	/**
	 * KVSの死活チェックを厳密にするか？
	 * @param check_client_availability_strictly
	 */
	public void setCheck_client_availability_strictly(
			boolean checkClientAvailabilityStrictly) {
		check_client_availability_strictly = checkClientAvailabilityStrictly;
		m_configuration.setProperty(CONFIG_KEY_CHECK_CLIENT_AVAILABILITY_STRICTLY,check_client_availability_strictly);
	}

	/**
	 * GET時のキーReadロックの取得待ち時間
	 * @return
	 */
	public int getGet_read_lock_timeout_millis() {
		return get_read_lock_timeout_millis;
	}

	/**
	 * SET時のキーReadロックの取得待ち時間
	 * @param get_read_lock_timeout_millis
	 * @throws Exception
	 */
	public void setGet_read_lock_timeout_millis(int getReadLockTimeoutMillis) throws StatusException {
		if(this.lighthouse!=null&&this.lighthouse.getStatus() != LightHouse.STATUS_STOP){
    		throw new StatusException(this.lighthouse.getStatus(),"LightHouse is running now.");
    	}
		get_read_lock_timeout_millis = getReadLockTimeoutMillis;
		m_configuration.setProperty(CONFIG_KEY_GET_READ_LOCK_TIMEOUT_MILLIS,get_read_lock_timeout_millis);
    }
		

	/**
	 *  キューサイズの制限
	 * @return
	 */
	public int getQueue_size_limit() {
		return queue_size_limit;
	}

	/**
	 *  キューサイズの制限
	 * @param queue_size_limit
	 */
	public void setQueue_size_limit(int queueSizeLimit){
		queue_size_limit = queueSizeLimit;
		m_configuration.setProperty(CONFIG_KEY_QUEUE_SIZE_LIMIT,queue_size_limit);
		if(this.lighthouse!=null){
			lighthouse.setQueueLimit(queueSizeLimit);
		}
	}
		

	/**
	 * キューサイズオーバー時のエンキュー待ち時間(ms)
	 * @return
	 */
	public long getEnqueue_timeout_millis() {
		return enqueue_timeout_millis;
	}

	/**
	 * キューサイズオーバー時のエンキュー待ち時間(ms) <br/>
	 * 0 = unlimited
	 * @param enqueue_timeout_millis
	 */
	public void setEnqueue_timeout_millis(long enqueueTimeoutMillis){
		enqueue_timeout_millis = enqueueTimeoutMillis;
		m_configuration.setProperty(CONFIG_KEY_ENQUEUE_TIMEOUT_MILLIS,enqueue_timeout_millis);
		if(this.lighthouse!=null){
			lighthouse.setEnqueueWaitTime(enqueueTimeoutMillis);
		}
	}
		
	/**
	 * バリューコンテンツの圧縮を有効にする。
	 * @return
	 */
	public boolean isCompress_values() {
		return compress_values;
	}

	/**
	 * バリューコンテンツの圧縮を有効にする。
	 * @param compress_values
	 */
	public void setCompress_values(boolean compressValues) {
		compress_values = compressValues;
		m_configuration.setProperty(CONFIG_KEY_COMPRESS_VALUES,compress_values);
	}

	/**
	 * バリューコンテンツの圧縮を有効時の圧縮メソッド
	 * @return
	 */
	public String getCompression_type() {
		return compression_type;
	}

	/**
	 * バリューコンテンツの圧縮を有効時の圧縮メソッド
	 * @param compression_type
	 * @throws StatusException 
	 */
	public void setCompression_type(String compressionType) throws StatusException{
		if(this.lighthouse!=null&&this.lighthouse.getStatus() != LightHouse.STATUS_STOP){
    		throw new StatusException(this.lighthouse.getStatus(),"LightHouse is running now.");
    	}
		if(CodingUtils.isValidCompressionType(compression_type)){
			compression_type = compressionType;
			compression_type_i = CodingUtils.getCompressionTypeCode(compressionType);
			m_configuration.setProperty(CONFIG_KEY_COMPRESSION_TYPE,compression_type);
		}
	}

	/**
	 * 圧縮を行うバイト数の閾値<br/>
	 * 圧縮メソッドによっては短いデータを圧縮すると圧縮後のバイト長が圧縮前よりも長くなることがあります。<br/>
	 * この値を超えるバイト長のバリューコンテンツを圧縮します。
	 * @return
	 */
	public int getCompression_threshold() {
		return compression_threshold;
	}

	/**
	 * 圧縮を行うバイト数の閾値<br/>
	 * 圧縮メソッドによっては短いデータを圧縮すると圧縮後のバイト長が圧縮前よりも長くなることがあります。<br/>
	 * この値を超えるバイト長のバリューコンテンツを圧縮します。
	 * @param compression_threshold
	 */
	public void setCompression_threshold(int compressionThreshold) {
		compression_threshold = compressionThreshold;
		m_configuration.setProperty(CONFIG_KEY_COMPRESSION_THRESHOLD,compression_threshold);
	}

	/**
	 * CAS(Compare and Store)を有効にする。
	 * @return
	 */
	public boolean isCas_enable() {
		return cas_enable;
	}

	/**
	 * CAS(Compare and Store)を有効にする。
	 * @param cas_enable
	 */
	public void setCas_enable(boolean casEnable) {
		cas_enable = casEnable;
		m_configuration.setProperty(CONFIG_KEY_CAS_ENABLE,casEnable);
	}

	public boolean isStore_primitives_as_string() {
		return store_primitives_as_string;
	}
	
	/**
	 * Group 毎の排他を行うか
	 * @return
	 */
	public boolean isLock_group() {
		return lock_group;
	}

	public void setLock_group(boolean lockGroup) {
		lock_group = lockGroup;
		m_configuration.setProperty(CONFIG_KEY_LOCK_GROUP,lockGroup);
	}
	
	/**
	 * 別VMインスタンスとのエントリー毎のロックを有効にする。
	 */
	public boolean isGlobal_lock_enable(){
		return global_lock_enable;
	}
	
	public  void setGlobal_lock_enable(boolean globalLockEnable){
		global_lock_enable = globalLockEnable;
		m_configuration.setProperty(CONFIG_KEY_GLOBAL_LOCK_ENABLE,global_lock_enable);
	}
	
	/**
	 * Global Lockのタイムアウト（ミリ秒）
	 * @return
	 */
	public long getGlobal_lock_timeout_millis(){
		return global_lock_timeout_millis;
	}
	
	public void setGlobal_lock_timeout_millis(long globalLockTimeoutMillis){
		global_lock_timeout_millis = globalLockTimeoutMillis;
		m_configuration.setProperty(CONFIG_KEY_GLOBAL_LOCK_TIMEOUT_MILLIS,global_lock_timeout_millis);
	}
	
	/**
	 * Global Lockのローカルポート（サーバーインタフェース）
	 * @return
	 */
	public String getGlobal_lock_local_bind(){
		return global_lock_local_bind;
	}
	
	public void setGlobal_lock_local_bind(String globalLockLocalBind){
		global_lock_local_bind = globalLockLocalBind;
		m_configuration.setProperty(CONFIG_KEY_GLOBAL_LOCK_LOCAL_BIND,global_lock_local_bind);
	}
	
	public InetSocketAddress getGlobalLockLocalBindAddress(){
		InetSocketAddress inetAddr = null;
		String host = "localhost";
		int port = 1976;
		
		if(global_lock_local_bind!=null&&global_lock_local_bind.length()>0){
			String[] splitted = global_lock_local_bind.split(":");
			if(splitted.length==1){
				String s_1 = splitted[0];
				try{
					port = Integer.parseInt(s_1);
				}catch(NumberFormatException e){
					host = s_1;
				}
			}else if(splitted.length==2){
				host = splitted[0];
				String s_2 = splitted[1];
				try{
					port = Integer.parseInt(s_2);
				}catch(NumberFormatException e){
				}
			}
			inetAddr = new InetSocketAddress(host,port);
		}
		return inetAddr;
	}
	
	/**
	 * Global Lockのリモートポート
	 * @return
	 */
	public String getGlobal_lock_remote_hosts(){
		return global_lock_remote_hosts;
	}
	
	public void setGlobal_lock_remote_hosts(String globalLockRemoteHosts){
		global_lock_remote_hosts = globalLockRemoteHosts;
		m_configuration.setProperty(CONFIG_KEY_GLOBAL_LOCK_REMOTE_HOSTS,global_lock_remote_hosts);
	}
	
	public InetSocketAddress[] getGlobalLockRemoteAddresses(){
		
		List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
		
		if(global_lock_remote_hosts!=null&&global_lock_remote_hosts.length()>0){
			String[] splitted_0 = global_lock_remote_hosts.split(",");
			if(splitted_0.length>0){
				
				for(int i=0;i<splitted_0.length;i++){
					if(splitted_0[i]!=null&&splitted_0[i].length()>0){
						
						String host = null;
						int port = 1976;
						
						String[] splitted = splitted_0[i].split(":");
						if(splitted.length==1){
							host = splitted[0];
						}else if(splitted.length==2){
							host = splitted[0];
							String s_2 = splitted[1];
							try{
								port = Integer.parseInt(s_2);
							}catch(NumberFormatException e){
							}
						}
						addresses.add(new InetSocketAddress(host,port));
					}
				}
			}
		}
		return addresses.toArray(new InetSocketAddress[addresses.size()]);
	}

	/**
	 * Client障害を無視する
	 * @return
	 */
	public boolean isIgnore_client_failure() {
		return ignore_client_failure;
	}

	public void setIgnore_client_failure(boolean ignoreClientFailure) {
		ignore_client_failure = ignoreClientFailure;
		m_configuration.setProperty(CONFIG_KEY_IGNORE_CLIENT_FAILURE,ignore_client_failure);
	}

	/**
	 * WEBコンソールを有効にする
	 * @return
	 */
	public boolean isConsole_enable() {
		return console_enable;
	}

	public void setConsole_enable(boolean consoleEnable) {
		console_enable = consoleEnable;
		m_configuration.setProperty(CONFIG_KEY_CONSOLE_ENABLE,console_enable);
	}

	/**
	 * Callback
	 */
	private LightHouse lighthouse = null;
	protected void setLightHouse(LightHouse lighthouse){
		this.lighthouse = lighthouse;
	}
}
