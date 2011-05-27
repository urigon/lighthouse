package jp.co.fujisan.lighthouse.client;

import java.io.File;
import java.rmi.server.UID;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import jp.co.fujisan.lighthouse.LightHouse;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class ClientFactory {
	
	private static final Log logger = LogFactory.getLog(ClientFactory.class);

    private static final Random random = new Random();
	
    public static final Integer VERSION = 1;
    private static final String PROP_KEY_NODE_NAME = "node.name";

	/*Client implementations*/
	public static final String SERVER_TYPE_MEMCACHED = "memcached";
	public static final String SERVER_TYPE_REDIS = "redis";
	public static final String SERVER_TYPE_TYRANT = "tyrant";
	public static final String SERVER_TYPE_DEBUG = "debug";
	public static String[] server_types = {SERVER_TYPE_MEMCACHED,SERVER_TYPE_REDIS,SERVER_TYPE_TYRANT,SERVER_TYPE_DEBUG};
	public static Integer[] weights = {1,2,3,4,5,6,7,8};
	
	private static final boolean isValidServerType(String server_type){

		if(server_type==null){
			return false;
		}
		
		for(int i=0;i<server_types.length;i++){
			if(server_type.equalsIgnoreCase(server_types[i])){
				return true;
			}
		}
		
		return false;
		
	}

	private static final int getNumber(XMLConfiguration config,String path){
		Object obj = config.getProperty(path);
	    if (obj == null) {
	    	return 0;
	    } else if (obj instanceof Collection) {
	    	return ((Collection) obj).size();
	    }
	    return 1;
	}

	protected static final int getIndex(XMLConfiguration config,int client_id){
		try{
			int num = getNumber(config,PROP_KEY_NODE_NAME);
			for(int index=0;index<num;index++){
				String key = "node("+index+").id";
				  try{
					  int id_value =  config.getInteger(key, 0);
					  if(id_value!=0&&id_value==client_id){
						  return index;
					  }
				  }catch(Exception ex){
					  continue;
				  }
			}
		}catch(Exception ex){
		}
		return -1;
	}

	
	protected static final List<Client> createClients(String ring_id,XMLConfiguration config,boolean ignoreClientFailure){
		
		List<Client> client_list = new ArrayList<Client>();
		
		String prop_key = PROP_KEY_NODE_NAME;
		int num = getNumber(config,prop_key);
		if(num>0){
			if(VERSION  != config.getInt("nodes[@version]", 0)){
				try{
		       		logger.warn("nodes.xml version is unmatched.");
		       		config.setProperty("nodes[@version]", VERSION);
		       		//FileUtils.copyFile(config.getFile(), new File(config.getBasePath()+File.separator+"nodes.versionumatched"));
		       		//config.clear();
		       		//num = getNumber(config,prop_key);
				}catch(Exception e){
				}
    		}
	        for(int n = 0 ; n < num ; n++){
	        	try{
	        	  Client client = ClientFactory.createClient(ring_id,config, n,ignoreClientFailure);
	        	  if(client!=null){
	            	  client_list.add(client); 
	        	  }
	           	}catch(Exception e){
	           		logger.debug("<Error>\r\n",e);
	        		logger.error(e);
	           	}
	        } 
		}
		return client_list;
	}
	
	protected static final Client createClient(String ring_id,XMLConfiguration config, int index,boolean ignoreFailure) throws Exception {
  	  
	  Integer id = null;
	  String server_type=null,key=null,name=null,host=null,failure=null;
	  int weight=1,host_port=0;
	  Map<String,Object> context = new HashMap<String,Object>();
	  /*
	   * Getting property on nodes.xml
	   * <nodes>
	   * 		<node *type="server_type">
	   * */
	  key = "node("+index+")[@type]";
	  try{
		  server_type = config.getString(key);
		  server_type = server_type.toLowerCase();
		  context.put("type", server_type);
	  }catch(Exception e){
		  logger.warn(key + "can't get.");
	  }
	  if(!isValidServerType(server_type)){
		  throw new ConfigurationException("Invalid client@type ["+server_type+"]");
	  }

	  /*
	   * Getting property on nodes.xml
	   * <nodes>
	   * 		<node type="server_type">
	   * 			*<name> 
	   * */
	  key = "node("+index+").name";
	  try{
		  name = config.getString(key);
		  context.put("name", name);
	  }catch(Exception e){
		  logger.warn(key + "can't get.");
	  }
	  
	  /*
	   * このノードが以前に失敗していたら、無視する。
	   * */
	  if(ignoreFailure){
		  context.remove("failure");
	  }else{
		  key = "node("+index+").failure";
		  try{
			  failure = config.getString(key);
		  }catch(Exception e){
			  logger.warn(key + "can't get.");
		  }
		  if(failure!=null&&failure.trim().length()>0){
			  logger.error("Node ["+name+"] has been failed ["+failure+"]. Please fix problem on this peer.");
			  context.put("failure", new ClientException(failure));
		  }
	  }
	  
	  /*
	   * Getting property on nodes.xml
	   * <nodes>
	   * 		<node type="server_type">
	   * 			*<weight> 
	   * */
	  key = "node("+index+").weight";
	  try{
		  weight = config.getInt(key);
		  context.put("weight", new Integer(weight).toString());
	  }catch(Exception e){
		  logger.warn(key + "can't get.");
	  }	  

	  /*
	   * Getting property on nodes.xml
	   * <nodes>
	   * 		<node type="server_type">
	   * 			<host *ip="ip_address"> 
	   * */
	  key = "node("+index+").host[@ip]";
	  try{
		  host = config.getString(key);
		  context.put("host", host);
	  }catch(Exception e){
		  logger.warn(key + "can't get.");
	  }

	  /*
	   * Getting property on nodes.xml
	   * <nodes>
	   * 		<node type="server_type">
	   * 			<host ip="ip_address" *port="port_number"> 
	   * */
	  key = "node("+index+").host[@port]";
	  try{
		  host_port = config.getInt(key);
		  context.put("host_port", new Integer(host_port).toString());
	  }catch(Exception e){
		  logger.warn(key + "can't get.");
	  }

	  /*
	   * Getting property on nodes.xml
	   * <nodes>
	   * 		<node type="server_type">
	   * 			*<id> 
	   * */
	  key = "node("+index+").id";
	  try{
		  id = config.getInteger(key,0);
	  }catch(Exception e){
		  logger.warn(key + "can't get.");
	  }
	  if(id==null||id==0){
		  /*
		   * Generate instance id 
		   */
		  id = genId(config);
	  }
	  context.put("id", name);
	  

	  Client client=null;
		try {
			  
			/*Now creating client. 
			 * */
			client = createClient(ring_id,config,id,server_type,name,weight,host,host_port,context);
			
			/* 
			 * クライアントが作成されたら、そのIDと作成日時を記録しておく。
			 * 停止、もしくはFail outした場合は、このエレメントは削除される。 
			 * */
			  if(client!=null)
			  {
				 String created =  new SimpleDateFormat("yyyy/MM/dd  HH:mm:ss z").format(new Date());
	
				/* 
				 * クライアントが作成されたら、そのIDと作成日時を記録しておく。
				 *  
				 * Setting property on nodes.xml
				 * <nodes>
				 * 			<node type="server_type">
				 * 				*<id> 
				 * 				*<created> 
				 * */
				  key = "node("+index+").id";
				  try{
					  config.setProperty(key, id);
				  }catch(Exception e){
					  logger.warn(key + ":"+id+" can't set.");
				  }
				  
				  key = "node("+index+").created";
				  try{
					  context.put("created",created);
					  config.setProperty(key, created);
				  }catch(Exception e){
					  logger.warn(key + ":"+created+" can't set.");
				  }
			  }
		  			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			logger.debug("<Error>\r\n",e1);
			/* もし生成に失敗したら、なんらかの設定ミスか環境依存の問題が考えられるため
			 * 失敗をマークして無効化する。
			 * 起動後の稼動中にFail outした場合にも、失敗をマークして無効化する。
			 * <failure>エレメントが存在するノード設定は、<failure>エレメントを削除しない限り無視される。 
			   * Setting property on nodes.xml
			   * <nodes>
			   * 	<node_type>
			   * 		<node type="server_type">
			   * 			*<failure> 
			   * */
			key = "node("+index+").failure";
			  try{
				  failure = new String(new SimpleDateFormat("yyyy/MM/dd  HH:mm:ss z").format(new Date()) + "  "+e1.getMessage());
				  context.put("failure", new ClientException(failure));
				  config.setProperty(key, failure );
			  }catch(Exception e){
				  logger.warn(key + ":"+e1.getMessage()+" can't set.");
			  }
			  throw e1;
		}
		
		
		try {
			config.save();
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			logger.debug("<Error>\r\n",e);
			logger.error("Configuration failed to save.",e);
		}
	  
	  return client;

	}
	
	protected static final Client createClient(String ring_id,XMLConfiguration config,Integer id,String server_type,String name,int weight,String host,int host_port,Map<String,Object> context) throws Exception{
	  	  
		  Client client = null; 
		  if(logger.isDebugEnabled()){
			  logger.debug("Constructing Client["+server_type+"]: Name=" + name + ", id="+id+", host="+host+":"+host_port );
		  }

		  try{
			  
			  if(context.get("failure")!=null){
				  /*
				   * 障害履歴のあるクライアントは、DummyClientインスタンスでプロパティのみアクセス可能にしておく。
				   */
				  client = new DummyClient(ring_id,name,id,server_type,context);
				  
			  }else{
				  
				  if(SERVER_TYPE_MEMCACHED.compareTo(server_type)==0){
					  client = new MemcachedClient(ring_id,name,id, weight,host,host_port,context);
				  }
				  if(SERVER_TYPE_REDIS.compareTo(server_type)==0){
					  client = new RedisClient(ring_id,name,id,weight,host,host_port,context);
				  }
				  if(SERVER_TYPE_TYRANT.compareTo(server_type)==0){
					  client = new TyrantClient(ring_id,name,id,weight,host,host_port,context);
				  }
				  if(SERVER_TYPE_DEBUG.compareTo(server_type)==0){
					  client = new DebugClient(ring_id,name,id,weight,context);
				  }
				  
			  }
			  
			return client;
			  
		  }catch(Exception e){
			  context.put("failure", new ClientException(e));
			  throw e;
		  }
	}
	
	/**
	 * 与えられたコンフィグレーションオブジェクトにクライアント設定情報のみを書き込みます。<br/>
	 * @param config コンフィグレーションオブジェクト
	 * @param id　ID null
	 * @param server_type　クライアントの種類 {@link #server_types}
	 * @param name 任意の名前
	 * @param weight　重み付け
	 * @param host　プライマリサーバーのIP
	 * @param host_port　プライマリサーバーのポート
	 * @param added
	 * @param created
	 * @param ex
	 * @throws ConfigurationException
	 */
	public synchronized static void addConfig(XMLConfiguration config,Integer id, String server_type,String name,int weight,String host,int host_port,Date added,Date created,Exception ex) throws ConfigurationException{

		server_type = server_type.toLowerCase();

		/*
		 * サーバーの実装タイプをチェック
		 */
		  if(!isValidServerType(server_type)){
			  throw new ConfigurationException("Invalid client@type ["+server_type+"]");
		  }		
		  
		  String nodeName =config.getRootElementName(); 
		  
		  //最初のエントリの場合は、Rootエレメントを"nodes"に指定。
			 if(nodeName.compareTo("configuration")==0){
				  try{
						 config.setRootElementName("nodes");
						 config.addProperty("nodes[@version]", VERSION);
				  }catch(Exception ignore){
					  
				  }
			 }
		 
		  /*
		   * クライアント名
		   * 
		   * Adding property to nodes.xml
		   * <nodes>の最初の子供に追加します。
		   * <nodes>
		   * 		<node type="server_type">
		   * 			*<name>
		   * */
		  String key = "node(-1).name";
		  try{
			   config.addProperty(key, name);
		  }catch(Exception e){
			  logger.warn(key + ":"+name+" can't set.");
		  }
		  
		  /*
		   * サーバーの実装タイプ
		   * 
		   * Adding property to nodes.xml
		   * <nodes>
		   * 		<node *type="server_type">
		   * */
		  key = "node[@type]";
		  try{
			   config.addProperty(key, server_type);
		  }catch(Exception e){
			  logger.warn(key + ":"+server_type+" can't set.");
		  }	  
		  
		  /*
		   * 重み付け（大きいと分散されるノード数が増えるため、ヒット率が高くなる）
		   * 
		   * Adding property to nodes.xml
		   * <nodes>
		   * 		<node type="server_type">
		   * 			*<weight>
		   * */
		  key = "node.weight";
		  try{
			   config.addProperty(key, weight);
		  }catch(Exception e){
			  logger.warn(key + ":"+weight+" can't set.");
		  }	  
		  
		  /*
		   * プライマリサーバーのIPアドレス
		   * SETオペレーションは、プライマリ＋マスタサーバー両方に行われる。
		   * GETオペレーションは、プライマリに最初に行われて、プライマリで見つけられない場合に、マスタに行われる。
		   * 
		   * Adding property to nodes.xml
		   * <nodes>
		   * 		<node type="server_type">
		   * 			<host *ip="ip_address">
		   * */
		  key = "node.host[@ip]";
		  try{
			   config.addProperty(key, host);
		  }catch(Exception e){
			  logger.warn(key + ":"+host+" can't set.");
		  }
		  
		  /*
		   * プライマリサーバーのポート
		   * 
		   * Adding property to nodes.xml
		   * <nodes>
		   * 		<node type="server_type">
		   * 			<host ip="ip_address" *port="port_number">
		   * */
		  key = "node.host[@port]";
		  try{
			   config.addProperty(key, host_port);
		  }catch(Exception e){
			  logger.warn(key + ":"+host_port+" can't set.");
		  }
		  
		  /*
		   * インスタンスID
		   * 一度追加されると一意のIDとしてその後
		   * クライアントに付与され続ける。
		   * 同じノードマッピングを保持しようとするなら、名前とIDは不変にしたほうがよい。
		   * データのノード分布に一貫性が保てなくなる。
		   * Adding property to nodes.xml
		   * <nodes>
		   * 		<node type="server_type">
		   * 			*<id>
		   * */
		  key = "node.id";
		  try{
			  config.addProperty(key, id);
		  }catch(Exception e){
			  logger.warn(key + ":"+id+" can't set.");
		  }
		  
		  /*
		   * 設定の追加日時
		   * 
		   * Adding property to nodes.xml
		   * <nodes>
		   * 		<node type="server_type">
		   * 			<added>
		   * */
		  key = "node.added";
		  try{
			  config.addProperty(key, new SimpleDateFormat("yyyy/MM/dd  HH:mm:ss z").format(added));
		  }catch(Exception e){
			  logger.warn(key + ":"+added+" can't set.");
		  }
		  
		  /*
		   * インスタンスの生成日時
		   * Fail outもしくはterminationがされた場合は、削除される。
		   * 
		   * Adding property to nodes.xml
		   * <nodes>
		   * 		<node type="server_type">
		   * 			<created>
		   * */
		  if(created!=null){
			  key = "node.created";
			  try{
				  config.addProperty(key, new SimpleDateFormat("yyyy/MM/dd  HH:mm:ss z").format(created));
			  }catch(Exception e){
				  logger.warn(key + ":"+created+" can't set.");
			  }
		  }
		  
		  /*
		   * 失敗マーク
		   * 最後に起動に失敗、fail out した場合に、その日時と原因が記録される。
		   * このエレメントが存在しているノードの設定は、無視され起動されない。
		   * 
		   * Adding property to nodes.xml
		   * <nodes>
		   * 		<node type="server_type">
		   * 			<failure>
		   * */
		  if(ex != null){
			key = "node.failure";
			  try{
				  config.addProperty(key, new SimpleDateFormat("yyyy/MM/dd  HH:mm:ss z").format(new Date()) +"  "+ex.getMessage() );
			  }catch(Exception e){
				  logger.warn(key + ":"+ex.getMessage()+" can't set.");
			  }
		  }

		try {
			config.save();
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			logger.debug("<Error>\r\n",e);
			logger.error("Configuration failed to save.",e);
		}
	  
	}
	
	public synchronized static void removeConfig(XMLConfiguration config,Integer id) throws ConfigurationException{
		
		try{
			int index = getIndex(config,id);
			String key = "node("+index+")";
			  try{
						/**
						 * 設定を削除
						 */
					  config.clearTree(key);
					  
			  }catch(Exception ex){
				logger.debug("<Error>\r\n",ex);
			  }
		}catch(Exception ex){
			logger.warn(ex);
		}finally{
			try {
				config.save();
			} catch (ConfigurationException e1) {
				// TODO Auto-generated catch block
				logger.warn(e1);
			}
		}

	}
	
	public synchronized static Client modConfig(String ring_id,XMLConfiguration config,Integer id,String server_type,String name,int weight,String host,int host_port,boolean ignoreFailure,boolean create) throws ConfigurationException{
		
		try{
			int index = getIndex(config,id);
			if(index<0){
				//No defined configuration
				throw new ConfigurationException("Configuration not found for client("+id+").");
			}
			
			String key = "node("+index+")";
			server_type = server_type.toLowerCase();

			/*
			 * サーバーの実装タイプをチェック
			 */
			  if(!isValidServerType(server_type)){
				  throw new ConfigurationException("Invalid client@type ["+server_type+"]");
			  }		
			  
			config.setProperty(key+".name", name);
			config.setProperty(key+"[@type]", server_type);
			config.setProperty(key+".weight", weight);
			config.setProperty(key+".host[@ip]", host);
			config.setProperty(key+".host[@port]", host_port);
			if(ignoreFailure){
				config.clearTree(key+".failure");
			}

			config.save();
			
			if(create){
				//start client with new configuration
				return createClient(ring_id,config,index,ignoreFailure);
			}
		}catch(Exception e){
			logger.error(e);
			throw new ConfigurationException(e);
		}

		return null;
	}
	
	protected static boolean markAvailable(XMLConfiguration config, Integer id ,boolean available){

		try{
			int index = getIndex(config,id);
			String key = "node("+index+").id";
			  try{
				  int id_value =  config.getInteger(key, 0);
				  if(id_value!=0&&id_value==id){
						/**
						 * 有効/無効を記録
						 */
					  key = "node("+index+").available";
					  config.setProperty(key, available );
					  return true;
				  }
			  }catch(Exception ex){
				  return false;
			  }
		}catch(Exception ex){
			logger.warn(ex);
		}finally{
			try {
				config.save();
			} catch (ConfigurationException e1) {
				// TODO Auto-generated catch block
				logger.warn(e1);
				  return false;
			}
		}
		return false;

	}

	protected static boolean markFailure(XMLConfiguration config, Integer id ,Exception e){

		try{
			int index = getIndex(config,id);
			String key = "node("+index+").id";
			  try{
				  int id_value =  config.getInteger(key, 0);
				  if(id_value!=0&&id_value==id){
						/**
						 * 無効を記録
						 */
					  key = "node("+index+").available";
					  config.setProperty(key, false );

					  /**
						 * 障害情報を記録
						 */
					  key = "node("+index+").failure";
					  config.setProperty(key, new SimpleDateFormat("yyyy/MM/dd  HH:mm:ss z").format(new Date()) + " "+e.getMessage() );
					  return true;
				  }
			  }catch(Exception ex){
				  return false;
			  }
		}catch(Exception ex){
			logger.warn(ex);
		}finally{
			try {
				config.save();
			} catch (ConfigurationException e1) {
				// TODO Auto-generated catch block
				logger.warn(e1);
				  return false;
			}
		}
		return false;
		
	}
	
	
	protected static boolean markTerminate(XMLConfiguration config, Integer id){

		try{
			int index = getIndex(config,id);
			String key = "node("+index+").id";
			  try{
				  int id_value =  config.getInteger(key, 0);
				  if(id_value!=0&&id_value==id){
						/**
						 * インスタンスIDを削除
						 */
					  //config.setProperty(key,"");
					  
						/**
						 * 無効を記録
						 */
					  key = "node("+index+").available";
					  config.setProperty(key, false );

						/**
						 * インスタンス作成日を削除
						 */
					  key = "node("+index+").created";
					  config.setProperty(key,"");
					  
					  return true;
				  }
			  }catch(Exception ex){
				  return false;
			  }
		}catch(Exception ex){
			logger.warn(ex);
		}finally{
			try {
				config.save();
			} catch (ConfigurationException e1) {
				// TODO Auto-generated catch block
				logger.warn(e1);
				  return false;
			}
		}
		return false;
		
	}	
	
	protected static final Integer genId(XMLConfiguration config){
		//return Long.toHexString(random.nextLong());
		Integer index = random.nextInt(Integer.MAX_VALUE);
		while(index==0||getIndex(config,index)>0){
			index = random.nextInt(Integer.MAX_VALUE);
		}
		return index;
	}
}
