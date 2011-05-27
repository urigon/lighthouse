package jp.co.fujisan.lighthouse.client;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import jp.co.fujisan.lighthouse.LightHouse;
import jp.co.fujisan.lighthouse.hashring.HashRing;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class ClientManager implements ClientEventListener{
	
	private static final Log logger = LogFactory.getLog(ClientManager.class);

	private XMLConfiguration m_config = null;
	
    private Map<Integer,Client> m_clients = new ConcurrentHashMap<Integer,Client>();
    private Map<Integer,Client> m_available_clients = new ConcurrentHashMap<Integer,Client>();
    
    public ClientManager() {
    }
    
    public ClientManager(XMLConfiguration config) {
    	m_config = config;
    }
    
    /**
   	 * Generating Ring
     * 
     * @param ring_id
     * @return
     */
    private HashRing generateRing(String ring_id,boolean ignoreClientFailure)
    {
    	/*
    	 * Terminate clients before generate new Ring
    	 */
		terminateClients();

    	List<Client> client_list = ClientFactory.createClients(ring_id,m_config,ignoreClientFailure);
    	Iterator<Client> ite= client_list.iterator();
    	while(ite.hasNext()){
    		Client client = ite.next();
    		client.setClientEventListener(this);
    	}
       return new HashRing(client_list);
    }
    
    /**
   	 * Generating Ring with configuration file.
     * @param ring_id
     * @param config
     * @return
     */
    public HashRing generateRing(String ring_id,XMLConfiguration config,boolean ignoreClientFailure){
    	m_config = config;
    	return this.generateRing(ring_id,ignoreClientFailure);
    }


    /**
     * Make clients available.
     * @param avaliable
     */
	public void availableClients(boolean avaliable){

		Iterator<Client> clients = getClients().iterator();
		while(clients.hasNext()){
			Client client = clients.next();
			if(client!=null){
				client.setAvailable(avaliable);
			}
		}
	}

	/**
	 * Terminates all clients.
	 */
	public void terminateClients(){

		Iterator<Client> clients = getClients().iterator();
		while(clients.hasNext()){
			Client client = clients.next();
			if(client!=null){
				client.terminate();
			}
		}
	}
	
	/**
	 * 有効・無効をマーク
	 * 無効の場合、Availableリストからは削除するが、リング、メモリには常駐
	 * RingにはClientレベルのオペレーションで戻れる
	 * @param client
	 * @param avaliable
	 */
	@Override
	public synchronized void  availableEvent(Client client,boolean avaliable){
		LightHouse lighthouse = LightHouse.getInstance(client.getRingId());
		if(lighthouse!=null){
			XMLConfiguration config = lighthouse.getStorage_configuration();
			Integer id = client.getId();

			if(avaliable){
				m_clients.put(client.getId(), client);
				m_available_clients.put(client.getId(), client);
				logger.info("Enabled client["+id+"].");
			}else{
				m_available_clients.remove(id);
				logger.info("Disabled client["+id+"].");
			}
			lighthouse.update_rt_get_retry_number();
			lighthouse.update_rt_replica_number();
			ClientFactory.markAvailable(config,id,avaliable);
		}
	}
	
	/**
	 * 障害情報を残す。
	 * Availableリストからは削除するが、メモリには常駐
	 * Ringからも削除され、アサインされていたNodeもRingから削除される。
	 * LightHouseレベルのStartが必要。
	 * 設定は残る
	 * @param client
	 * @param e
	 */
	@Override
	public synchronized void  failEvent(Client client,Exception e){
		LightHouse lighthouse = LightHouse.getInstance(client.getRingId());
		if(lighthouse!=null){

			XMLConfiguration config = lighthouse.getStorage_configuration();
			Integer id = client.getId();
			logger.error("Unrecoverable failure detected on client["+client.getId()+"].",e);
			m_available_clients.remove(id);
			lighthouse.update_rt_get_retry_number();
			lighthouse.update_rt_replica_number();
			ClientFactory.markFailure(config,id,e);
		}
		
	}

	/**
	 * メモリー上から、破棄
	 * Ringからも削除され、アサインされていたNodeもRingから削除される。
	 * LightHouseレベルのStartが必要。
	 * 設定は残る
	 * @param client
	 */
	@Override
	public synchronized void  terminateEvent(Client client){
		LightHouse lighthouse = LightHouse.getInstance(client.getRingId());
		if(lighthouse!=null){
			XMLConfiguration config = lighthouse.getStorage_configuration();
			Integer id = client.getId();
			logger.info("Termination detected on client["+id+"].");
			m_clients.remove(id);
			m_available_clients.remove(id);
			lighthouse.update_rt_get_retry_number();
			lighthouse.update_rt_replica_number();
			ClientFactory.markTerminate(config,id);
		}
	
	}

	/**
	 * Get all clients
	 * @return
	 */
	public List<Client> getClients(){
		return new LinkedList<Client>(m_clients.values());
	}

	/**
	 * Get client with id
	 * @param client_id
	 * @return
	 */
	public Client getClient(Integer client_id){
		return m_clients.get(client_id);
	}

	/**
	 * Get number of client instances 
	 * @return
	 */
	public int getClientsNumber(){
		return m_clients.size();
	}

	/**
	 * Get available clients
	 * @return
	 */
	public List<Client> getAvailableClients(){
		return new LinkedList<Client>(m_available_clients.values());
	}
	
	/**
	 * Get number of available client instances 
	 * @return
	 */
	public int getAvailableClientsNumber(){
		return m_available_clients.size();
	}
	
	/**
	 * 新規クライアントを作成します。<br/>
	 * インスタンスの作成後、成功失敗に関係なく設定情報を設定ファイルに書き込みます。<br/>
	 * 新しいクライアントインスタンスは、nameとランダムIDによってLightHosueクラウド上で一意のインデックスにマップされます。<br/>
	 * @param config コンフィグレーションオブジェクト
	 * @param server_type　クライアントの種類 {@link #server_types}
	 * @param name 任意の名前
	 * @param weight　重み付け
	 * @param host　プライマリサーバーのIP
	 * @param host_port　プライマリサーバーのポート
	 * @return
	 * @throws Exception　クライアント実装のコンストラクタでスローされる例外
	 * @see {@link #server_types} {@link #addConfig(XMLConfiguration, Integer, String, String, int, String, int, String, int, Date, Date, Exception)}
	 */
	public Client addClient(String ring_id,String server_type,String name,int weight,String host,int host_port,boolean clear_before_join,boolean ignoreClientFailure) throws Exception{
		
		//Create Client inst. also add configuration.
		Integer id = ClientFactory.genId(m_config);
		Date instanciated_date = new Date();
		ClientFactory.addConfig(m_config,id, server_type, name, weight, host, host_port, instanciated_date,null,null);		
		int index = ClientFactory.getIndex(m_config,id);
		if(index<0){
			//No defined configuration
			throw new ConfigurationException("Configuration failed of client("+id+").");
		}
		  
		Client client = ClientFactory.createClient(ring_id,m_config,index,ignoreClientFailure);
		if(client!=null){
			m_clients.put(client.getId(), client);
			client.setClientEventListener(this);
			logger.info("New Client created. : "+client.toString());
			return client;
		}
		return null;
		
	}
	
	
}
