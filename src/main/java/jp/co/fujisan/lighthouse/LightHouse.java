package jp.co.fujisan.lighthouse;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import jp.co.fujisan.KVS;
import jp.co.fujisan.lighthouse.client.Client;
import jp.co.fujisan.lighthouse.client.ClientFactory;
import jp.co.fujisan.lighthouse.client.ClientManager;
import jp.co.fujisan.lighthouse.exception.StatusException;
import jp.co.fujisan.lighthouse.hashring.AbstractNode;
import jp.co.fujisan.lighthouse.hashring.HashRing;
import jp.co.fujisan.lighthouse.hashring.HashRingIterator;
import jp.co.fujisan.lighthouse.hashring.Node;
import jp.co.fujisan.lighthouse.lock.GlobalLockClientManager;
import jp.co.fujisan.lighthouse.lock.GlobalLockServer;
import jp.co.fujisan.lighthouse.queue.KVQueue;
import jp.co.fujisan.lighthouse.queue.KVQueueLockingImpl;
import jp.co.fujisan.lighthouse.queue.QueueItem;
import jp.co.fujisan.lighthouse.queue.ThreadFreeReadWriteLock;
import jp.co.fujisan.lighthouse.queue.exception.AlreadyFinalizedException;
import jp.co.fujisan.lighthouse.queue.exception.EnqueueTimeoutException;
import jp.co.fujisan.lighthouse.queue.exception.LockTimeoutException;
import jp.co.fujisan.lighthouse.queue.exception.NoMoreCapacityException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.event.ConfigurationEvent;
import org.apache.commons.configuration.event.ConfigurationListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;


public class LightHouse implements KVS, InitializingBean, DisposableBean{

	private static final Log logger = LogFactory.getLog(LightHouse.class);

    public static ClassLoader CLASSLOADER = null;
	
	private final static String META_DELIMITER = ":"; 
	private final static String COMPRESSION_HEADER_PREFIX = "c="; 
	
	/*
	 *  instances are held with ring_id;
	 */
	private static Map<String,LightHouse> instances = new HashMap<String,LightHouse>();
	
	public static LightHouse getInstance(String ringId){
		return instances.get(ringId);
	}
	private static void setInstance(String ringId,LightHouse instance){
		instances.put(ringId,instance);
	}
	
	private static LightHouse removeInstance(String ringId){
		if(ringId==null){
			instances.clear();
			return null;
		}
		return instances.remove(ringId);
	}
	

	/**
	 * Status that LightHouse cloud is stopping, means follow.</br>
	 * - LightHouse instance is available.</br>
	 * - Configuration is available.</br>
	 * - HashRing is not constructed.</br>
	 * - All Clients are not constructed.</br>
	 * - Neither SET nor DELETE executor threads are not constructed.</br>
	 * - Neither SET nor DELETE queues are not constructed.</br>
	 */
	public static final int STATUS_STOP = 0;

	/**
	 * Status that LightHouse is trying to start, means follow.</br>
	 * - LightHouse instance is available.</br>
	 * - Configuration is not available partially.</br>
	 * - HashRing is constructing.</br>
	 * - Clients is constructing.</br>
	 * - Both SET and DELETE executor threads are constructing.</br>
	 * - Both SET and DELETE queues are constructing.</br>
	 * - All operations are not accessible. They will throw java.lang.Exeption.
	 */
	public static final int STATUS_START_INPROGRESS = 1;

	/**
	 * Status that LightHouse is running, means follow.</br>
	 * - LightHouse instance is available.</br>
	 * - Configuration is not available partially.</br>
	 * - HashRing is available.</br>
	 * - Clients is available.</br>
	 * - Both SET and DELETE executor threads are available.</br>
	 * - Both SET and DELETE queues are available.</br>
	 * - All operations are accessible.
	 */
	public static final int STATUS_RUNNING = 2;

	/**
	 * Status that LightHouse is trying to stop, means follow.</br>
	 * - LightHouse instance is available.</br>
	 * - Configuration is not available partially.</br>
	 * - HashRing is destructing.</br>
	 * - Clients is destructing.</br>
	 * - Both SET and DELETE executor threads are destructing.</br>
	 * - Both SET and DELETE queues are destructing.</br>
	 * - All operations are not accessible. They will throw java.lang.Exeption.
	 */
	public static final int STATUS_STOP_INPROGRESS = 3;
	
	private volatile int m_status = STATUS_STOP;

    private ClientManager client_manager = null; 
    public ClientManager getClientManager(){
    	return client_manager;
    }
	
    private Configurations configurations = null;
    
    private HashRing m_ring = null;
    
    /*
     * KeyLock でキーレベルでR/Wロック
     */
	private KVQueue m_queue = null;
	
	/*
	 * group レベル排他
	 */
	private ConcurrentHashMap<String,CountDownLatch> group_locks = null; 
    
	/*
	 * ノード間リモートロック 
	 */
	private GlobalLockServer lock_daemon = null;
	private GlobalLockClientManager lock_client = null;
	
    //有効クライアント数によって増減するので、設定値と別の変数を確保
    private int rt_replica_number = Configurations.CONFIG_DEFAULT_REPLICA_NUMBER;
    private ExecutorService  m_cmd_executor = null;
    private List<CommandTask> m_cmd_tasks = null;
    
    //有効クライアント数によって増減するので、設定値と別の変数を確保
	private int rt_get_retry_number = Configurations.CONFIG_DEFAULT_GET_RETRY_NUMBER;

	/*
	 * 圧縮済みデータヘッダ
	 * format c=$cfg_compression_type:
	 */
	private String rt_compression_header = COMPRESSION_HEADER_PREFIX+Configurations.CONFIG_DEFAULT_COMPRESSION_TYPE+META_DELIMITER;
	
	public LightHouse(){
	}

	public void setConfigurations(Configurations configurations){
		this.configurations = configurations;
	}
	public Configurations getConfiguraions(){
		return this.configurations;
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		this.setup();
	}
	@Override
	public void destroy() throws Exception {
		LightHouse lightHouse = removeInstance(this.configurations.getRing_id());
		if(lightHouse!=null){
			lightHouse.terminate();
		}
		lightHouse = null;
	}
	
    public synchronized void start() throws Exception {
        if(m_status == STATUS_RUNNING||m_status == STATUS_START_INPROGRESS){
        	throw new StatusException(m_status,"Already running.");
        }

    	logger.info("---------------------------------------------------------");
    	m_status = STATUS_START_INPROGRESS;
        logger.info("STATUS_START_INPROGRESS");
    	
        try{

    		//コマンドキャッシュ
			m_queue = new KVQueueLockingImpl(this.configurations.get_read_lock_timeout_millis);
        	m_queue.setQueueLimit(this.configurations.queue_size_limit);
        	m_queue.setEnqueueWaitTime(this.configurations.enqueue_timeout_millis);
        	
        	//グループロック
        	group_locks = new ConcurrentHashMap<String,CountDownLatch>();
        		
			/*
			 * コマンドスレッドの準備
			 * */
        	if(this.configurations.commit_async){
    			m_cmd_executor = Executors.newFixedThreadPool(this.configurations.async_thread_number);
    			//コマンドスレッド
    			m_cmd_tasks = new LinkedList<CommandTask>();
    			for(int i=0;i<this.configurations.async_thread_number;i++){
    				CommandTask thread = new CommandTask("CommandTask-"+i);
    				m_cmd_tasks.add(thread);
    				m_cmd_executor.execute(thread);
    			}
        	}
			
        	//HashRIngをClientとともに作成
			m_ring = client_manager.generateRing(this.configurations.ignore_client_failure);
			
			/*
			 * Ring上のClientをひとつずつ有効化
			 */
			if(m_ring.size()>0){
				HashRingIterator ite = new HashRingIterator(m_ring);
				while(ite.hasNext()){
					Node node = ite.next();
					if(node!=null){
						Client client = node.getClient();
						  if(client!=null&&!client.isAvailable(false)){
								if(!client.setAvailable(true)){
									//起動に失敗したら、Ringから削除
									m_ring.removeClient(client.getId());
								}
						  }
					}
				}
			}
			
			//SETの最大レプリカ数を決定
			update_rt_replica_number();
			//GETの最大試行数を決定
			update_rt_get_retry_number();
			
			//リモートロックの準備
			if(this.configurations.isGlobal_lock_enable()){
				lock_daemon = new GlobalLockServer(this.configurations.getGlobalLockLocalBindAddress());
				lock_daemon.setExipreTimeout(this.configurations.getGlobal_lock_timeout_millis());
				try{
					lock_daemon.startup();
				}catch(java.net.BindException bind){
					logger.warn(bind);
				}
				lock_client = new GlobalLockClientManager(this.configurations.getGlobalLockRemoteAddresses());
				lock_client.startup();
			}else{
				lock_daemon = new GlobalLockServer();
				lock_daemon.setExipreTimeout(this.configurations.getGlobal_lock_timeout_millis());
			}
			
        	m_status = STATUS_RUNNING;
			logger.info("STATUS_RUNNING");
        	
        }catch(Exception e){
			logger.error("<ERROR>Failed to start. stopped by force.\r\n",e);
			try{
				stop(true); 
			}catch(Exception ignore){
			}finally{
	        	m_status = STATUS_STOP;
			}
        	throw e;
        }finally{
			//m_ring.dump("lookup_ring@start");
        }
    }

    public void stop(boolean force)throws Exception {
    	
        logger.info("LightHouse: attempt to Stopping...");
		if(m_status == STATUS_STOP){
        	throw new StatusException(m_status,"LightHouse is already stopped.");
		}
    	
        if(this.configurations.force_shutdown){
        	force = true;
        }
        
        try{
        	if(force){
                logger.info("LightHouse: Stopping by force...");
                m_status = STATUS_STOP_INPROGRESS;

    			if(logger.isDebugEnabled()){
                	logger.debug("CommandQueue("+(m_queue==null?"0":m_queue.queueSize())+")");
    			}
    			
//        		if(this.isLock_group()){
//        			synchronized(group_locks){
//        				Iterator<CountDownLatch> ite = group_locks.values().iterator();
//        				while(ite.hasNext()){
//        					CountDownLatch latch = ite.next();
//        					latch.notifyAll();
//        				}
//        			}

    			/*
                 * COMMANDスレッドの強制終了処理
                 * */
                if(m_cmd_executor!=null){
        			if(logger.isDebugEnabled()){
                        logger.debug("m_cmd_executor shutting down...");
                    	logger.debug("CommandThread("+(m_cmd_tasks==null?"0":m_cmd_tasks.size())+") ");
        			}
                	try{
                		m_queue.terminate();
                		m_cmd_executor.shutdownNow();
                	}catch(Exception e){
                		logger.warn("<Warn>\r\n",e);
                	}finally{
                        logger.debug("m_cmd_executor shut down complete.");
                	}
                }
                

                /*
                 * HashRingのクリア
                 * */
                if(m_ring!=null){
                    try{
            			m_ring.clear();
                    }catch(Exception e){
                    	logger.warn("<Warn>\r\n",e);
                    }
                    logger.debug("HashRing cleared.");
                }
                
                /*
                 * Clientの無効化
                 */
                client_manager.availableClients(false);
                logger.debug("Clients disabled.");
                
            }else{
            	synchronized(this){
            		if(m_status == STATUS_START_INPROGRESS){
                    	throw new StatusException(m_status,"LightHouse is going to start.");
            		}
                    
                    logger.info("LightHouse: Stopping...");
                    m_status = STATUS_STOP_INPROGRESS;
                    
            		/*
            		 * 未処理キューの処理を待つ
            		 */
            		while((m_queue!=null&&m_queue.queueSize()>0)){
            			if(logger.isDebugEnabled()){
                        	logger.debug("CommandQueue("+(m_queue==null?"0":m_queue.queueSize())+")");
            			}
        				Thread.sleep(1000);
            		}
            		
                    /*
                     * COMMANDスレッドの終了処理
                     * */
                    if(m_cmd_executor!=null){
            			if(logger.isDebugEnabled()){
                            logger.debug("m_cmd_executor shutting down...");
                        	logger.debug("CommandThread("+(m_cmd_tasks==null?"0":m_cmd_tasks.size())+") ");
            			}
                    	try{
                    		m_queue.terminate();
                    		Iterator<CommandTask> ite = m_cmd_tasks.iterator();
                    		while(ite.hasNext()){
                    			CommandTask task = ite.next();
                    			if(task!=null){
                    				try{
                        				task.shutdown();
                    				}catch(Exception e){
                    					logger.warn("<Warn>\r\n",e);
                    				}
                    			}
                    		}
                    		m_cmd_executor.shutdownNow();
                    	}catch(Exception e){
                    		logger.warn("<Warn>\r\n",e);
                    	}finally{
                    		//m_set_executor = null;
                    		//m_set_queue = null;
                            logger.debug("m_cmd_executor shut down complete.");
                    	}
                    }
                    
                    /*
                     * HashRingのクリア
                     * */
                    if(m_ring!=null){
                        try{
                			m_ring.clear();
                        }catch(Exception e){
                        	logger.warn("<Warn>\r\n",e);
                        }
                        logger.debug("HashRing cleared.");
                    }
                    
                    /*
                     * Clientの無効化
                     */
                    client_manager.availableClients(false);
                    logger.debug("Clients disabled.");

            	}
            }

        	//グループロックのクリア
        	if(group_locks!=null){
                group_locks.clear();
                group_locks = null;
        	}

			//リモートロックのクリア
			if(lock_daemon!=null){
				lock_daemon.destroy();
			}
			if(lock_client!=null){
				lock_client.destroy();
			}
            
        	m_status = STATUS_STOP;
            logger.info("LightHouse: stopped.");
        }catch(Exception e){
        	logger.warn("<Warn>\r\n",e);
        }

    }
    
    public void terminate(){
    	try{
        	if(this.m_status != STATUS_STOP){
        		this.stop(true);
        	}
        	client_manager.terminateClients();
        	m_ring = null;
            m_cmd_executor = null;
            if(m_cmd_tasks!=null){
                m_cmd_tasks.clear();
                m_cmd_tasks = null;
            }
            if(m_queue!=null){
            	try {
    				m_queue.terminate();
    			} catch (Exception ignore) {
    			}
    			m_queue = null;
            }
    	} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
            try {
            	this.configurations.save();
			} catch (Exception e) {
				logger.error(e);
			}
			//removeInstance(ring_id);
            logger.info("LightHouse:"+this.configurations.ring_id+" terminated.");
    	}
    	
    }
    
	public synchronized Integer joinServer(Client client) throws Exception{
		if(m_status == STATUS_START_INPROGRESS){
			throw new StatusException(m_status,"Lighthouse status is STATUS_START_INPROGRESS Now.");
		}

		if(client!=null){
			if(m_status == STATUS_RUNNING){
				this.m_ring.join(client);
	        }
			return client.getId();
		}
		return null;
		
	}
	public synchronized Integer addServer(String server_type,String name,int weight,String host,int host_port,boolean clear_before_join) throws Exception{
		if(m_status == STATUS_START_INPROGRESS){
			throw new StatusException(m_status,"Lighthouse status is STATUS_START_INPROGRESS Now.");
		}
		
		Client client = client_manager.addClient(server_type, name, weight, host, host_port, clear_before_join,this.configurations.ignore_client_failure);
		  
		if(m_status == STATUS_RUNNING){
			if(client.setAvailable(true)){
				if(clear_before_join){
					try{
						client.clear();
					}catch(Exception ignore){
					}
				}
				this.m_ring.join(client);
			}
        }
		
		return client.getId();

	}
    /**
     * 
     * @param server_id
     * @param terminate
     * @param delete_config
     * @throws Exception
     */
	public synchronized void removeServer(Integer server_id,boolean terminate,boolean delete_config) throws Exception{
		
		if(m_status == STATUS_START_INPROGRESS){
			throw new StatusException(m_status,"Lighthouse status is STATUS_START_INPROGRESS Now.");
		}
		Client client = null;
        if(m_status == STATUS_RUNNING){
        	//Remove from ring.
    		client = this.m_ring.removeClient(server_id);
        }
		if(client == null){
			client = client_manager.getClient(server_id);
		}
		if(client!=null&&(delete_config||terminate)){
			//terminate.
			client.terminate();
		}
		/*
		 * 設定ファイルからエントリを削除
		 */
		if(delete_config){
			ClientFactory.removeConfig(this.configurations.m_storage_configuration,server_id);
		}
		
	}
	public synchronized Integer modServer(Integer server_id,String server_type,String name,int weight,String host,int host_port,boolean ignoreFailure) throws Exception{
		if(m_status == STATUS_START_INPROGRESS){
			throw new StatusException(m_status,"Lighthouse status is STATUS_START_INPROGRESS Now.");
		}
		
		//Terminate Server on runtime.
		removeServer(server_id,true,false);
			
		//mod configuration also create Client inst .
		Client client = ClientFactory.modConfig(this.configurations,server_id, server_type, name, weight, host, host_port, ignoreFailure,true);

		if(m_status == STATUS_RUNNING){
    		if(client!=null){
				if(client.setAvailable(true)){
					this.m_ring.join(client);
				}
    		}
        }
		if(client!=null){
			return client.getId();
		}else{
			return null;
		}
		
	}
    /**
     * ノードグループ配下のエントリをDELETEする。
	 * 通常のDELETEをgroupで指定した近傍ノードから行う。
	 * ノードグループを指定してSETしたキーを検索する場合や、
	 * どのノードにキーが存在するかが明確な場合に利用する。
     * @param group
     * @param key
     * @return
     * @throws Exception
     */
	@Override
    public boolean deleteGroup(String group)throws Exception{

		Set<String> keys = keys(null,group+META_DELIMITER);
		if(keys!=null){
			wait_group(group,keys.size());
			Iterator<String> ite = keys.iterator();
			while(ite.hasNext()){
				String key = ite.next();
		    	delete(key);
			}
			return true;
		}
		return false;
    }
	
    public boolean delete(String group,String[] keys)throws Exception{

		if(keys!=null){
			wait_group(group,keys.length);
			for(String key: keys){
		    	delete(group+META_DELIMITER+key);
			}
			return true;
		}
		return false;
    }

    /**
     * ノードグループを指定してDELETEする。
	 * 通常のDELETEをgroupで指定した近傍ノードから行う。
	 * ノードグループを指定してSETしたキーを検索する場合や、
	 * どのノードにキーが存在するかが明確な場合に利用する。
     * @param group
     * @param key
     * @return
     * @throws Exception
     */
	@Override
    public boolean delete(String group,String key)throws Exception{
		/*
		 * キーにグループサフィックスをつける
		 * (groupキーもインデックスの一部でKVSノード上の名前空間の拡張に使われる。)
		 */
		wait_group(group,1);
    	key = group+META_DELIMITER+key;
    	return delete(key);
    }

    /**
	 *TODO
	 *DELETEオペレーションで確実にCloud上からエントリを削除しようとしたら、
	 *Ring上のすべてのサーバーにDELETEコマンドを実行する必要がある。
	 *相対的に時間がかかるのでSETと同様に一時キャッシュにDELETEDキーを保管して
	 *GETシーケンスはそこをまずチェックする。
	 *DELETEデーモンスレッドが非同期で一時キャッシュ上のDELETEキーを取得し
	 *Cloudを巡回して一括削除する。
	 * 
	 * @param key
	 * @return
	 * @throws Exception
	 */
	@Override
	public boolean delete(String key)throws Exception{
		if(m_status != STATUS_RUNNING){
    		throw new StatusException(m_status,"LightHouse is not started yet.");
    	}

		if(key==null||key.trim().length()==0){
			throw new IllegalArgumentException("null key is not acceptable.");
		}
		
		QueueItem item = new QueueItem(QueueItem.CMD_DELETE,key,this.configurations.enqueue_timeout_millis);
		if(this.configurations.commit_async){
			//非同期Deleteはエンキューして終わり
			this.m_queue.enqueue(item);
			return true;
		}else{
			try{
				//同期DeleteはキーをReadロックしておく 
				this.m_queue.put(key,item);
				return delete_internal(key);
			}finally{
				try{
					item.unlock();
				}finally{
					this.m_queue.remove(key);
				}
			}
		}
    }
	private boolean delete_internal(String key)throws Exception{
		
    	boolean result = false;
    	
    	//long index = this.m_ring.genIndex(key);

    	try{
    		
    		/*
    		 * 有効クライアントをすべてDELETE
    		 * */
    		int try_number = client_manager.getAvailableClientsNumber();
    		
    		Set<Integer> triedClients = new HashSet<Integer>();
    		
    		/*
    		 * グループIDの切り出し
    		 * format is "$group_id:$key"
    		 */
    		String group = null;
    		int pos = key.indexOf(META_DELIMITER);
    		if(pos>0){
    			group = key.substring(0, pos);
    		}
    		
    		/*
    		 * RingIDが設定されている場合は、プリフィックスとして追加
    		 */
    		if(this.configurations.ring_id!=null){
    			group= this.configurations.ring_id+"."+group;
    			key= this.configurations.ring_id+"."+key;
    		}
    		

    		HashRingIterator ite = new HashRingIterator(this.m_ring,group);
    		while(triedClients.size()<=try_number&&ite.hasNext()){
				Node node = ite.next();
				
				if(node!=null&&node.isAvailable()){
	        		Client client = node.getClient();
					if(client!=null&&client.isAvailable(this.configurations.check_client_availability_strictly)){

						Integer clientId = client.getId();
						if(!triedClients.contains(clientId))
						{
							try {
								if(client.delete(key)){
									result = true;
								}
							} catch (UnsupportedOperationException e) {
								logger.warn("Client("+clientId+")UnsupportedOperationException:"+e.getMessage());
							} catch (Exception e) {
								/*TODO ここでの例外をどう処理するか？
								 * */
								logger.error("<Error>\r\n",e);
							}
							triedClients.add(clientId);
						}
					}
				}
			}        	
    	}catch(Exception e){
    		logger.debug("<Error>\r\n",e);
    		logger.error("DELETE<"+key+">"+ " aborted. caused by ["+e.toString()+"]");
    		throw e;
    	}finally{
//    		if(this.isLock_group()){
//        		int pos = key.indexOf(META_DELIMITER);
//        		if(pos>0){
//        			String group = key.substring(0, pos);
//        			CountDownLatch latch = this.group_locks.get(group);
//        			if(latch!=null){
//        				latch.countDown();
//        				if(latch.getCount()==0){
//        					group_locks.remove(group);
//        				}
//        			}
//        		}
//    		}
    	}
    	
    	return result;
		
	}
	/**
	 * keyの近傍ノードから検索を開始。
	 * 以下のどちらかの条件を満たすまで順次GETを行う。
	 * 1,対象キーの発見。
	 * 2,有効サーバーのホップ数を消化する。
	 * 3,Ringの最後までトラバースする。
	 * TODO 効率を考えるとRingを一巡するのは合理的でない。
	 * SETの拡散範囲を考えるとその1.5から2倍くらいのホップ数で十分か？
	 * 
	 * @param key
	 * @return
	 * @throws Exception
	 */
	@Override
	public Object get(String key)throws Exception{
		String[] keys = {key};
		Map<String,Object> result = get(null,keys);
		return result.get(key);
	}
	/**
	 * 複数キーをGETする。
	 * 各keyの近傍ノードから検索を開始。
	 * 以下のどちらかの条件を満たすまで順次GETを行う。
	 * 1,すべての対象キーの発見。
	 * 2,各キーごとに有効サーバーのホップ数を消化する。
	 * 3,各キーごとにRingの最後までトラバースする。
	 * TODO へたするとキーごとにRingを検索する羽目になるので、処理時間がかかる可能性が高い。
	 * 
	 * @param key
	 * @return
	 * @throws Exception
	 */
	@Override
	public Map<String,Object> mget(String[] keys)throws Exception{
		return get(null,keys);
	}

	/**
     * ノードグループを指定してGETする。
	 * 通常のGETをgroupで指定した近傍ノードから行う。
	 * ノードグループを指定してSETしたキーを検索する場合や、
	 * どのノードにキーが存在するかが明確な場合に利用する。
     * @param group
     * @param key
     * @return
     * @throws Exception
     */
	@Override
    public Object get(String group,String key)throws Exception{
		String[] keys = {key};
		Map<String,Object> result = this.get(group,keys);
		return result.get(key);
    }
	
    private Map<String,Object> get(String group,String[] keys)throws Exception{

		if(m_status != STATUS_RUNNING){
			throw new StatusException(m_status,"LightHouse is not started yet.");
    	}
    	
		String prefix = "";
		if(this.configurations.ring_id!=null){
			prefix = this.configurations.ring_id+".";
		}
		if(group!=null&&group.length()>0){
			group += META_DELIMITER;
			prefix += group;
		}
			
    	try{

    		Map<String,Object> result = new HashMap<String,Object>();
    		List<String> node_keys = new LinkedList<String>(); 
    		
    		/*
    		 * キャッシュを検索
    		 */
			for(int i=0;i<keys.length;i++){
				String key = keys[i];
				if(key==null||key.trim().length()==0){
					continue;
				}
	    		
				QueueItem item = new QueueItem(QueueItem.CMD_GET,group+key,this.configurations.enqueue_timeout_millis);
				try{
					QueueItem qedItem =  this.m_queue.enqueue(item);
					if(qedItem!=null){
						if(QueueItem.CMD_DELETE==qedItem.command()){
							result.put(keys[i], null);
						}else if(QueueItem.CMD_SET==qedItem.command()){
							result.put(keys[i], qedItem.value());
						}
					}else{
			    		/*
			    		 * キャッシュで見つからないキーは、ノード検索のために追加
			    		 * RingIDが設定されている場合は、プリフィックスとして追加
			    		 */
		    			node_keys.add(prefix+key);
					}
				}catch(LockTimeoutException e){
					logger.warn("GET<"+keys[i]+">"+ " LockTimeoutException.");
				}
			}
			
			//System.out.println("Found on cache="+result.toString());
			if(node_keys.size()==0){
				return result;
			}
    		
    		/*
    		 * リングを検索
    		 */
    		Map<String,Object> result_on_nodes = new HashMap<String,Object>();
    		if(group!=null){
        		/*
        		 * グループ指定の場合は、"group"の最近傍ノードから
        		 * KVSノード（Client）単位でMultiGetを実行していく。
        		 * KVSノード単位で全キーが取得される。
        		 * 適切なグループをしている場合は、1回で取得可能。
        		 * すべてのキーが取得できなくても、対象KVSノードでひとつでもエントリーが存在すれば返る。
        		 */
        		//試行済みKVSノードリスト
            	Set<Integer> triedClients = new HashSet<Integer>();
    	    	HashRingIterator ite = new HashRingIterator(this.m_ring,prefix);
    			while(ite.hasNext()&&(triedClients.size()==0||triedClients.size()<this.rt_get_retry_number)){
    				Node node = ite.next();
    				if(node!=null&&node.isAvailable()){
    	        		Client client = node.getClient();
    					if(client!=null&&client.isAvailable(this.configurations.check_client_availability_strictly)){
    						Integer clientId = client.getId();
    						if(!triedClients.contains(clientId))
    						{
    							try {
    								if(node_keys.size()==1){
    									/*
    									 * キーが1件だと通常のGet
    									 * この方が効率がよい
    									 */
    									String key = node_keys.get(0);
        								Object value = client.get(key);
        				        	    if(value!=null){
        				        	    	result_on_nodes.put(key, filter(value));
        				        	    	break;
        				        	    }else{
        				        	    	//System.err.println(curKey+"=null");
        				        	    	continue;
        				        	    }
    								}else{
    									/*
    									 * MultiGetをコール
    									 * Protocolレベルで実装されていれば効率がよい。
    									 * KVSClient実装に依存する。
    									 */
    									result_on_nodes = client.get(node_keys.toArray(new String[node_keys.size()]));
    								}
    							} catch (UnsupportedOperationException e) {
    								logger.warn("Client("+clientId+")UnsupportedOperationException:"+e.getMessage());
    							} catch (Exception e) {
    								logger.debug("<Error>\r\n",e);
    							}
    							triedClients.add(clientId);
    						}
    					}
    				}
    			}
        	}else{
        		if(client_manager.getAvailableClientsNumber()==1)
        		{
            		/*
            		 * 有効KVSノードがひとつしか無い場合は、MultiGet実行
            		 */
        			List<Client> clients = client_manager.getAvailableClients();
	        		Client client = clients.get(0);
					if(client!=null&&client.isAvailable(this.configurations.check_client_availability_strictly)){
						try {
							if(node_keys.size()==1){
								/*
								 * キーが1件だと通常のGet
								 * この方が効率がよい
								 */
								String key = node_keys.get(0);
								Object value = client.get(key);
				        	    if(value!=null){
				        	    	result_on_nodes.put(key, filter(value));
				        	    }
							}else{
								/*
								 * MultiGetをコール
								 * Protocolレベルで実装されていれば効率がよい。
								 * KVSClient実装に依存する。
								 */
								result_on_nodes = client.get(node_keys.toArray(new String[node_keys.size()]));
							}
						} catch (UnsupportedOperationException e) {
							logger.warn("Client("+client.getId()+")UnsupportedOperationException:"+e.getMessage());
						} catch (Exception e) {
							logger.debug("<Error>\r\n",e);
						}
					}
    		    	
        		}else{
            		/*
            		 * キーのみの指定の場合は、クラウドに分散されている各キーをRingで検索していく。
            		 * 時間がかかる。
            		 */
        			Iterator<String> keysite = node_keys.iterator();
        			while(keysite.hasNext()){
        				String key = keysite.next();
        	    		//試行済みKVSノードリスト
        	        	Set<Integer> triedClients = new HashSet<Integer>();
        		    	HashRingIterator ite = new HashRingIterator(this.m_ring,key);
    	    			while(ite.hasNext()&&(triedClients.size()==0||triedClients.size()<this.rt_get_retry_number)){
        					Node node = ite.next();
        					if(node!=null&&node.isAvailable()){
        		        		Client client = node.getClient();
        						if(client!=null&&client.isAvailable(this.configurations.check_client_availability_strictly)){
        							Integer clientId = client.getId();
        							if(!triedClients.contains(clientId))
        							{
        								try {
        									Object value = client.get(key);
        					        	    if(value!=null){
        					        	    	/*
        					        	    	 * すべてのキーに対して試行する。
        					        	    	 */
        					        	    	result_on_nodes.put(key, filter(value));
        					        	    	break;
        					        	    }
        								} catch (UnsupportedOperationException e) {
        									logger.warn("Client("+clientId+")UnsupportedOperationException:"+e.getMessage());
        								} catch (Exception e) {
        									logger.debug("<Error>\r\n",e);
        								}
        								triedClients.add(clientId);
        							}
        						}
        					}
        				}
        			}
        		}
        	}
			
			//System.out.println("Found on ring="+result_on_nodes.toString());
			if(result_on_nodes.size()>0){
				//Merge with cache result.
				Iterator<String> node_keys_ite = result_on_nodes.keySet().iterator();
				while(node_keys_ite.hasNext()){
					String node_key = node_keys_ite.next();
					if(node_key.startsWith(prefix)){
						String original_key = node_key.substring(prefix.length(),node_key.length()); 
						result.put(original_key,result_on_nodes.get(node_key));
					}
				}
			}
			return result;
			
    	}catch(Exception e){
    		logger.debug(e);
    		logger.error("GET<"+Arrays.toString(keys)+">"+ " aborted. caused by ["+e.toString()+"]");
    		throw e;
    	}
    }
    private Object filter(Object value){
    	if(true){
        	try{
        		String str = value.toString();
        		if(str.startsWith(COMPRESSION_HEADER_PREFIX)){
        			int s = str.indexOf(COMPRESSION_HEADER_PREFIX)+COMPRESSION_HEADER_PREFIX.length();
        			int e = str.indexOf(META_DELIMITER,s);
        			int compession_type = Integer.parseInt(str.substring(s,e)); 
        			String compressed_value = str.substring(++e);
        			return CodingUtils.extract(compressed_value, compession_type);
        		}
        	}catch(Exception e){
        		logger.error(e);
        	}
    	}
		return value;
    }
	/**
	 * ノードグループを指定してSETする。
	 * 通常のSETをgroupで指定した近傍ノードに行う。
	 * 異なるキーでも一定のノードに限定してストアされる。
	 * ノードグループを指定してSETしたキーは、
	 * ノードグループを指定したGETをしないとヒット率が低下するため
	 * 比例して検索時間が長くなる。
	 * 
	 * @param group
	 * @param key
	 * @param value
	 * @return
	 * @throws Exception
	 */
	@Override
    public Long set(String group,String key,Object value)throws Exception{
		/*
		 * キーにグループサフィックスをつける
		 * (groupキーもインデックスの一部でKVSノード上の名前空間の拡張に使われる。)
		 */
			
		wait_group(group,1);
		key = group+META_DELIMITER+key;
    	return this.set(key,value);
    	
    }
	
	/**
	 * keyの近傍ノードにSET<br/>
	 * 通常モード(cfg_set_async==false)：1件のSETが成功したら、レプリケーションはSetTaskで行う。<br/>
	 * 非同期モード(cfg_set_async==true)：すべてのSETはSetTaskで行う。キューにエントリを追加して終了。<br/>
	 * 
	 * @param key
	 * @param value
	 * @return
	 * @throws Exception 
	 * @throws  
	 * @throws EnqueueTimeoutException 
	 * @throws Exception
	 */
	@Override
    public Long set(String key,Object value) throws Exception{
		if(m_status != STATUS_RUNNING){
    		throw new StatusException(m_status,"LightHouse is not started yet.");
    	}

		if(key==null||key.trim().length()==0){
			throw new IllegalArgumentException("null key is not acceptable.");
		}
		
    	QueueItem item = new QueueItem(QueueItem.CMD_SET,key,value,this.configurations.enqueue_timeout_millis);
		if(this.configurations.commit_async){
			this.m_queue.enqueue(item);
			return (long)0;
		}else{
			try{
				this.m_queue.put(key,item);
		    	return set_internal(key,value);
			}finally{
				try{
					item.unlock();
				}finally{
					this.m_queue.remove(key);
				}
			}
		}
    }
    
    private Long set_internal(String key,Object value)throws Exception {
    	if(key==null||key.trim().length()==0){
    		throw new Exception("empty key can't accept.");
    	}
    	
    	Long headNodeId = (long)-1;
    	try{
    		
    		/*
    		 * グループIDの切り出し
    		 * format is "$group_id:$key"
    		 */
    		String group = null;
    		int pos = key.indexOf(META_DELIMITER);
    		if(pos>0){
    			group = key.substring(0, pos+1); 
    		}
    		
    		/*
    		 * RingIDが設定されている場合は、プリフィックスとして追加
    		 */
    		if(this.configurations.ring_id!=null){
    			group= this.configurations.ring_id+"."+group;
    			key= this.configurations.ring_id+"."+key;
    		}
    		
    		/*
    		 * バリューコンテンツを圧縮
    		 */
    		if(this.configurations.store_primitives_as_string&&this.configurations.compress_values&&value.toString().getBytes().length>this.configurations.compression_threshold){
    			try{
    				//圧縮タイプを指定して
        			value = this.rt_compression_header + CodingUtils.compress(value.toString(),this.configurations.compression_type_i);
    			}catch(Exception e){
    				logger.error(e);
    				//圧縮せず
    				/*
    				 * もとのvalueの先頭にthis.cfg_compression_type + ":"が入っていたら？
    				 * extractでエラー発生→OK?
    				 */
    			}
    		}

    		/*
    		 * グループIDによって先頭ノードを検索する 
    		 */
    		Set<Integer> storedClients = new HashSet<Integer>();
    		HashRingIterator ite = new HashRingIterator(this.m_ring,group);
			while(storedClients.size()<this.rt_replica_number&&ite.hasNext()){
				Node node = ite.next();
				
				if(node!=null&&node.isAvailable()){
	        		Client client = node.getClient();
					if(client!=null&&client.isAvailable(this.configurations.check_client_availability_strictly)){

						Integer clientId = client.getId();
						if(!storedClients.contains(clientId))
						{
							try {
								if(client.set(key,value)){
									if(headNodeId<0){
										headNodeId = node.getIndex();
									}
									storedClients.add(clientId);
								}
							} catch (UnsupportedOperationException e) {
								logger.warn("Client("+clientId+")UnsupportedOperationException:"+e.getMessage());
							} catch (Exception e) {
								/*TODO ここでの例外をどう処理するか？
								 * */
								logger.debug("<Error>\r\n",e);
							}
						}
					}
				}
			}
    	}catch(Exception e){
    		logger.debug("<Error>\r\n",e);
    		logger.error("SET<"+key+"="+(value==null?"null":value.toString())+">"+ " aborted. caused by ["+e.toString()+"]");
    		throw e;
    	}finally{
//    		if(this.isLock_group()){
//        		int pos = key.indexOf(META_DELIMITER);
//        		if(pos>0){
//        			String group = key.substring(0, pos);
//        			CountDownLatch latch = this.group_locks.get(group);
//        			if(latch!=null){
//        				latch.countDown();
//        				if(latch.getCount()==0){
//        					group_locks.remove(group);
//        				}
//        			}
//        		}
//    		}
    	}
    	return headNodeId;
    }
    
    
    /**
     * 非同期SETオペレーションスレッド
     * @author development
     *
     */
    public class CommandTask extends Thread
    {
    	private volatile boolean run = true;

    	public CommandTask(String thread_name){
    		super(thread_name);
    		this.run = true;
    	}

    	public void shutdown() throws InterruptedException {
    		run = false;
    	}
    	
    	public void run(){
    		
    		while(run){
    			QueueItem item = null;
    			try{
        			item = m_queue.dequeue();
        			if(item!=null){
        				switch(item.command()){
           				case QueueItem.CMD_GET:
           					break;
           				case QueueItem.CMD_SET:
            				set_internal(item.key(),item.value());
           					break;
           				case QueueItem.CMD_DELETE:
            				delete_internal(item.key());
           					break;
           				default:
        				}
        			}
    			}catch(LockTimeoutException e){
    			}catch(AlreadyFinalizedException e){
					break;
				} catch (Exception e) {
					logger.error("<Error>\r\n",e);
    			}finally{
					if(item!=null){
	    				try {
	    					item.commit();
						} catch (Exception e) {
						}
    					item.unlock();
					}
    			}
    		}
			logger.info("SetTask("+this.hashCode()+") exit.");
    		synchronized(this){
    			notify();
    		}
    	}
    }
    
    /**
     * Returns status code.
     * 
     * @return int STATUS_RUNNING, STATUS_START_INPROGRESS, STATUS_STOP, STATUS_STOP_INPROGRESS<br/>
     * @see  jp.co.fujisan.lighthouse.LightHouse#STATUS_RUNNING
     * @see  jp.co.fujisan.lighthouse.LightHouse#STATUS_START_INPROGRESS
     * @see  jp.co.fujisan.lighthouse.LightHouse#STATUS_STOP
     * @see  jp.co.fujisan.lighthouse.LightHouse#STATUS_STOP_INPROGRESS
     */
    public int getStatus(){
    	return m_status;
    }
    
	/**
	 * 未処理リクエスト数
	 * The number of requests in asynchronous process queue.
	 * @return
	 */
	public int getPendingRequestNumber(){
		return getQueueSize();	
	}
	
	/**
	 * 修理中リクエスト数
	 * The number of requests in process.
	 * @return
	 */
	public int getInprocessRequestNumber(){
		return getCacheSize() - getQueueSize(); 
	}
	
	/**
	 * For debug and test
	 * @return
	 */
	protected int getQueueSize(){
		if(	this.m_queue!=null){
			return 	this.m_queue.queueSize();
		}
		return 0;
	}
	
	/**
	 * For debug and test
	 * @return
	 */
	protected int getCacheSize(){
		if(	this.m_queue!=null){
			return 	this.m_queue.cacheSize();
		}
		return 0;
	}
	
	/**
	 * SET時のレプリカのコピー数(cfg_replica_number)のランタイム変数<br/>
	 * 有効クライアント数により、動的に変化する<br/>
	 * cfg_replica_numberよりも大きい数値はセットできない。<br/>
	 * 有効クライアント数-1よりも大きい数値はセットできない。<br/>
	 */
	public void update_rt_replica_number() {

		//SETの最大レプリカ数は有効クライアント数まで。
		int max_rep_num = client_manager.getAvailableClientsNumber();

		//設定値が有効クライアント数より大きい場合は、有効クライアント数に合わせる。
		//設定値がマイナスの場合は、有効クライアント数に合わせる。
		if(max_rep_num==0 || configurations.replica_number>max_rep_num || configurations.replica_number<=0){
			rt_replica_number = max_rep_num;
		}else{
			rt_replica_number = configurations.replica_number;
		}
		
		if(logger.isDebugEnabled()){
			logger.debug("rt_replica_number="+rt_replica_number);
		}
		
	}
	
	/**
	 * GET時の最大試行ノード数(cfg_get_retry_number)のランタイム変数<br/>
	 * 有効クライアント数により、動的に変化する<br/>
	 * cfg_get_retry_numberよりも大きい数値はセットできない。<br/>
	 * 有効クライアント数よりも大きい数値はセットできない。<br/>
	 */
	public void update_rt_get_retry_number() {

		//GETの最大試行数は有効クライアント数まで。
		int max_ret_num = client_manager.getAvailableClientsNumber(); 
		
		//設定値が有効クライアント数より大きい場合は、有効クライアント数に合わせる。
		//設定値がマイナスの場合は、有効クライアント数に合わせる。
		if(max_ret_num==0 || configurations.get_retry_number>max_ret_num || configurations.get_retry_number<=0){
			rt_get_retry_number = max_ret_num;
		}else{
			rt_get_retry_number = configurations.get_retry_number;
		}
		if(logger.isDebugEnabled()){
			logger.debug("rt_get_retry_number="+rt_get_retry_number);
		}
		
	}


	/**
	 * For debug and test
	 * @return
	 */
	public List<Node> getNodesAsList(Integer client_id){
		List<Node> list = new ArrayList<Node>();
		try{
			if(m_ring==null){
				logger.warn("Ring is not generated yet.");
			}else{
				HashRingIterator ite = new HashRingIterator(m_ring);
				while(ite.hasNext()){
					Node node = ite.next();
					if(node!=null){
						int clientId = node.getClient().getId();
						if(client_id==null||client_id.intValue()==clientId){
							//System.out.println(node.getName());
							list.add(node);
						}
					}
				}
			}
		}catch(Exception e){
			logger.debug("<Error>\r\n",e);
			logger.error("error on getNodesAsList; " + e.getMessage());
		}
		return list;
	}
	
	public void setup() throws ConfigurationException, StatusException{
		
		/*
		 * Configurations is required.
		 */
		if(configurations==null){
			throw new ConfigurationException("Nothing Configurations set to property. You must to set Configurations instance before setup.");
		}
		
		configurations.setLightHouse(this);
		
    	/*
    	 * Setting up runtime properties.
    	 */
    	client_manager = new ClientManager(this);
		rt_compression_header = LightHouse.COMPRESSION_HEADER_PREFIX+this.configurations.compression_type_i+META_DELIMITER;
    	CLASSLOADER = Thread.currentThread().getContextClassLoader();

    	setInstance(this.configurations.getRing_id(),this);
		
    }

	/**
	 *  キューサイズの制限
	 * @param queue_size_limit
	 */
	protected void setQueueLimit(int queueSizeLimit){
		if(m_queue!=null){
			m_queue.setQueueLimit(queueSizeLimit);
		}
	}
		
	/**
	 * キューサイズオーバー時のエンキュー待ち時間(ms) <br/>
	 * 0 = unlimited
	 * @param enqueue_timeout_millis
	 */
	protected void setEnqueueWaitTime(long enqueueTimeoutMillis){
		if(m_queue!=null){
			m_queue.setEnqueueWaitTime(enqueueTimeoutMillis);
		}
	}

	@Override
	public Map<String, Object> getGroup(String group) throws Exception {
		Map<String,Object> result = new HashMap<String,Object>();
		Set<String> keys = keys(group,null);
		if(keys!=null&&keys.size()>0){
			result = this.get(group, keys.toArray(new String[keys.size()]));
		}
		return result;
	}

	@Override
	public Map<String, Object> getGroup(String group, String prefix)
			throws Exception {
		Map<String,Object> result = new HashMap<String,Object>();
		Set<String> keys = keys(group,prefix);
		if(keys!=null&&keys.size()>0){
			result = this.get(group, keys.toArray(new String[keys.size()]));
		}
		return result;
	}
	
    /**
     * ノードグループを指定して複数キーをGETする。
	 * マルチGETをgroupで指定した近傍ノードから行う。
	 * ノードグループを指定してSETしたキーを検索する場合や、
	 * どのノードにキーが存在するかが明確な場合に利用する。
     * @param group
     * @param key
     * @return
     * @throws Exception
     */
	@Override
	public Map<String, Object> getGroup(String group, String[] keys)
			throws Exception {
		if(keys!=null&&keys.length>0){
			return this.get(group, keys);
		}
		return getGroup(group);
	}

	@Override
	public int count(String group, String keyPrefix) throws Exception {
		Set<String> keys = keys(group,keyPrefix);
		if(keys!=null){
			return keys.size();
		}
		return 0;
	}

	@Override
	public int count(String keyPrefix) throws Exception {
		Set<String> keys = keys(null,keyPrefix);
		if(keys!=null){
			return keys.size();
		}
		return 0;
	}

	@Override
	public Set<String> keys(String keyPrefix) throws Exception {
		// TODO Auto-generated method stub
		return keys(null,keyPrefix);
	}

	/**
	 * GroupからkeyPrefixをプリフィックスにしているキーを検索
	 * group指定時に、groupノードで見つからなければ終了
	 * group指定なしの場合は、リング全体を検索しマージしたキーリストを返す。
	 */
	@Override
	public Set<String> keys(String group,String keyPrefix) throws Exception {
		
		if(m_status != STATUS_RUNNING){
			throw new StatusException(m_status,"LightHouse is not started yet.");
    	}
		
		String prefix = this.configurations.getRing_id()+".";
		if(group!=null&&group.length()>0){
			prefix += group + META_DELIMITER;
		}
		String pattern = prefix;
		if(keyPrefix!=null&&keyPrefix.length()>0){
			pattern += keyPrefix;
		}

		Set<String> keys  = new HashSet<String>();
		try{
			//Groupノードのみを検索（rt_get_retry_numberは無視）
			if(group!=null&&group.length()>0){
		    	HashRingIterator ite = new HashRingIterator(this.m_ring,prefix);
				Node node = ite.next();
				if(node!=null&&node.isAvailable()){
	        		Client client = node.getClient();
					if(client!=null&&client.isAvailable(this.configurations.check_client_availability_strictly)){
						Integer clientId = client.getId();
						try {
							keys = client.keys(pattern);
		        	    	if(logger.isDebugEnabled()){
								logger.debug("keys of "+pattern+"* ="+keys.toString());
		        	    	}
						} catch (UnsupportedOperationException e) {
							logger.warn("Client("+clientId+")UnsupportedOperationException:"+e.getMessage());
						} catch (Exception e) {
							logger.debug("<Error>\r\n",e);
						}
					}
				}
			}else{
				//Group指定無のため、全ノードを検索してマージする。
		    	HashRingIterator ite = new HashRingIterator(this.m_ring,pattern);
		    	
	    		Set<Integer> triedClients = new HashSet<Integer>();
				while(ite.hasNext()){
					Node node = ite.next();
					if(node!=null&&node.isAvailable()){
		        		Client client = node.getClient();
						if(client!=null&&client.isAvailable(this.configurations.check_client_availability_strictly)){
							Integer clientId = client.getId();
							if(triedClients.contains(clientId)){
								continue;
							}
							triedClients.add(clientId);
							try {
								keys.addAll(client.keys(pattern));
			        	    	if(logger.isDebugEnabled()){
									logger.debug("keys of "+pattern+"* ="+keys.toString());
			        	    	}
							} catch (UnsupportedOperationException e) {
								logger.warn("Client("+clientId+")UnsupportedOperationException:"+e.getMessage());
							} catch (Exception e) {
								logger.debug("<Error>\r\n",e);
							}
						}
					}
				}
			}
			if(keys!=null){
				Set<String> ret  = new HashSet<String>();
				Iterator<String> ite = keys.iterator();
				while(ite.hasNext()){
					String key = ite.next();
					String sub = key.substring(prefix.length()); 
					ret.add(sub);
				}
				return ret;
			}
		} catch (Exception e) {
			logger.error(e);
		}
    	return null;
    }

	@Override
	public Long setGroup(String group, Map<String, Object> entries)
			throws Exception {
		
		wait_group(group,entries.size());
		Iterator<Entry<String,Object>> ite = entries.entrySet().iterator();
		while(ite.hasNext()){
			Entry<String,Object> entry = ite.next();
	    	this.set(group+META_DELIMITER+entry.getKey(),entry.getValue());
		}
		return new Long(0);
	}

	private void wait_group(String group,int num) throws LockTimeoutException, InterruptedException{
//		if(this.isLock_group()){
//			String key = this.ring_id +"." +group;
//			while(true){
//				synchronized(group_locks){
//					CountDownLatch latch = this.group_locks.get(key);
//					if(latch!=null){
//						if(this.getGet_read_lock_timeout_millis()>0){
//							if(!latch.await(this.getGet_read_lock_timeout_millis(), TimeUnit.MILLISECONDS))
//							{
//								throw new LockTimeoutException();
//							}
//						}else{
//							latch.await();
//						}
//						continue;
//					}
//					group_locks.put(key,new CountDownLatch(num));
//					break;
//				}
//			}
//		}
	}

	@Override
	public long lock(String key) throws Exception {
		long id = Thread.currentThread().getId();
		lock(id,key);
		return id;
	}

	@Override
	public void lock(long id,String key) throws Exception {
		lock_daemon.lock(id, key);
		if(this.configurations.isGlobal_lock_enable()&&lock_client!=null){
			lock_client.lock(id,key);
		}
	}

	@Override
	public long lock(String group, String key) throws Exception {
		long id = Thread.currentThread().getId();
		lock(id,group,key);
		return id;
	}
	@Override
	public void lock(long id,String group, String key) throws Exception {
		lock_daemon.lock(id,group, key);
		if(this.configurations.isGlobal_lock_enable()&&lock_client!=null){
			lock_client.lock(id,group,key);
		}
	}

	@Override
	public long lockGroup(String group) throws Exception {
		long id = Thread.currentThread().getId();
		lockGroup(id,group);
		return id;
	}
	
	@Override
	public void lockGroup(long id,String group) throws Exception {
		lock_daemon.lock(id,group, null);
		if(this.configurations.isGlobal_lock_enable()&&lock_client!=null){
			lock_client.lock(id,group,null);
		}
	}
	
	@Override
	public long unlock(String key) throws Exception {
		long id = Thread.currentThread().getId();
		unlock(id,key);
		return id;
	}

	@Override
	public void unlock(long id,String key) throws Exception {
		if(this.configurations.isGlobal_lock_enable()&&lock_client!=null){
			lock_client.unlock(id,key);
		}
	}

	@Override
	public long unlock(String group, String key) throws Exception {
		long id = Thread.currentThread().getId();
		unlock(id,group,key);
		return id;
	}

	@Override
	public void unlock(long id,String group, String key) throws Exception {
		lock_daemon.unlock(id,group, key);
		if(this.configurations.isGlobal_lock_enable()&&lock_client!=null){
			lock_client.unlock(id,group,key);
		}
	}

	@Override
	public long unlockGroup(String group) throws Exception {
		long id = Thread.currentThread().getId();
		unlockGroup(id,group);
		return id;
	}
	
	@Override
	public void unlockGroup(long id,String group) throws Exception {
		lock_daemon.unlock(id, group,null);
		if(this.configurations.isGlobal_lock_enable()&&lock_client!=null){
			lock_client.unlock(id,group,null);
		}
	}

}
