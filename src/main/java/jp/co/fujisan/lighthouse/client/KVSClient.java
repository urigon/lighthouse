package jp.co.fujisan.lighthouse.client;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
//import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public abstract class KVSClient extends LocalClient {

	private static final Log logger = LogFactory.getLog(KVSClient.class);

	protected InetSocketAddress address;
	
	
	private Map<Integer,Integer> m_failure_count = new ConcurrentHashMap<Integer,Integer>();
    
	/*
	 * Connection pool
	 */
	protected ConnectionPool conn_pool = null;
	final static protected int POOL_INIT_SIZE = 1;
	final static protected int POOL_MIN_SIZE = 10;
	final static protected int POOL_MAX_SIZE = 100;
	
	private static Timer pool_sweeper =  null;
	protected static ConnectionPoolSweeperTask pool_sweeper_task = null;
	final static protected int POOL_SWEEP_INTERVAL = 60000; //1min
	
	/**
	 * 
	 * @param name
	 * @param id
	 * @param weight
	 * @param host
	 * @param host_port
	 * @param context
	 * @throws Exception
	 */
	public KVSClient(String name, Integer id, int weight,String host,int host_port,Map<String,Object> context) throws Exception {
		super(name,id,weight,context);
		try{
			
			if(host!=null&&host.length()>0){
				address = new InetSocketAddress(host,host_port);
			}
			conn_pool = new KVSClient.SocketConnectionPool(address);
			
		}catch(Exception e){
			this.onFail(e);
			throw e;
		}
		this.isAvailable = false;
	}
	
	public KVSClient(String host,int host_port) throws Exception {
		try{
			if(host!=null&&host.length()>0){
				address = new InetSocketAddress(host,host_port);
			}
			conn_pool = new KVSClient.SocketConnectionPool(address);
		}catch(Exception e){
			this.onFail(e);
			throw e;
		}
		this.isAvailable = false;
	}

	public InetSocketAddress getHost() {
		// TODO Auto-generated method stub
		return this.address;
	}

	public void setHost(String host,int port) throws UnknownHostException {
		// TODO Auto-generated method stub
		this.address = new InetSocketAddress(host,port);
	}

	@Override
	public String toString(){
		String s_host = "unknown";
		int s_host_port = 0;
		if(this.address!=null){
			s_host = this.address.getHostName();
			s_host_port = this.address.getPort();
		}
		return super.toString() + " " +
				"host="+s_host+ ":" + s_host_port;
	}

	protected Socket getSocket(){
		return ((KVSClient.SocketConnectionPool)conn_pool).getSocket();
	}
	
	protected void releaseSocket(Socket socket){
		((KVSClient.SocketConnectionPool)conn_pool).releaseSocket(socket);
	}

	/**
	 * @throws Exception 
	 */
	protected void connect() throws Exception{
		try{
			conn_pool.init();
		}catch(Exception e){
			this.onFail(e);
			throw e;
		}
		if(pool_sweeper==null){
			pool_sweeper = new Timer("ConnectionPoolSweeper");
			if(pool_sweeper_task==null){
				pool_sweeper_task = new ConnectionPoolSweeperTask();
			}
			pool_sweeper.schedule(pool_sweeper_task,POOL_SWEEP_INTERVAL, POOL_SWEEP_INTERVAL);
		}
		pool_sweeper_task.addConnectionPool(conn_pool);
	}
	
	protected void disconnect(){
		if(pool_sweeper_task!=null){
			pool_sweeper_task.removeConnectionPool(conn_pool);
			if(conn_pool!=null){
				conn_pool.clear();
			}
			if(pool_sweeper_task.size()==0){
				pool_sweeper_task.cancel();
				pool_sweeper_task=null;
				pool_sweeper.cancel();
				pool_sweeper.purge();
				pool_sweeper=null;
			}
		}
	}
	
	@Override
	public boolean isAvailable(boolean isStrictly){

		if(!isAvailable){
			return false;
		}
		
		if(isStrictly){
			
			/*
			 * 厳密モード
			 */
			try {
				
				 InetSocketAddress address = new InetSocketAddress(this.address.getAddress(),this.address.getPort());
                 Socket s = new Socket(address.getAddress(),address.getPort());
                 if(logger.isDebugEnabled()){
	                 logger.debug("Checking host_socket(" + address.getAddress()+ ":" + address.getPort()+")");
                 }
                 s.close();
 				isAvailable = true;
				return true;
				
             }catch (Exception ex) {
                 // The remote host is not listening on this port
            	 logger.debug("<Error>\r\n",ex);
              }
			
			isAvailable = false;
			this.onFail(new SocketException("No longer available host_socket(" + this.address==null?"null":this.address.getAddress()+ ":" + this.address==null?"null":this.address.getPort()+")"));
			
		}
		
		return isAvailable;
	}

	@Override
	public boolean setAvailable(boolean available){
		
		if(this.isAvailable==available){
			return this.isAvailable;
		}
		
		this.isAvailable = available;
		
		try{

			if(this.isAvailable){
				connect();
			}else{
				disconnect();
			}
			
		}catch(Exception e){
			this.onFail(e);
			return false;
		}
		
		if(listener!=null){
			listener.availableEvent(this,this.isAvailable);
		}

		
		return this.isAvailable;

	}

	@Override
	public void terminate(){
		
		this.isAvailable = false;
		
		try{
			/*
			 * リソースの開放
			 */
			this.disconnect();
			this.address = null;
			this.conn_pool = null;
		}catch(Exception ex){
			logger.debug("<Error>\r\n",ex);
		}
		if(listener!=null){
			listener.terminateEvent(this);
		}
		
	}
	

	/**
	 * 可能な限りのリソースを開放して、障害状態の情報を残す。
	 * @param e
	 */
	protected void onFail(Exception e){

		//一定回数以内の失敗であれば無視
		Integer failureCount = m_failure_count.get(this.getId());
		if(failureCount==null){
			failureCount = 1;
		}else{
			failureCount++;
		}
		m_failure_count.put(this.getId(),failureCount);
		if(failureCount!=null&&4>failureCount){
			return;
		}
		
		this.isAvailable = false;
		
		logger.debug("<Error>\r\n",e);
		
		try{
			
			/*
			 * 情報の収集
			 */
			//StringBuffer buff = new StringBuffer();
			//buff.append(this.toString());
			//buff.append("\r\n");
			//buff.append(e.getMessage());
			this.context.put("failure", new ClientException(e));
			

			/*
			 * リソースの開放
			 */
			this.disconnect();
			this.address = null;
			this.conn_pool = null;
		}catch(Exception ex){
		}
		
		if(listener!=null){
			listener.failEvent(this, e);
		}

	}
	
	static class ConnectionPoolSweeperTask extends TimerTask
	{
		List<ConnectionPool> pools = null;
		
		public ConnectionPoolSweeperTask(){
			pools = new LinkedList<ConnectionPool>();
		}

		@Override
		public void run() {
			synchronized(pools){
				Iterator<ConnectionPool> ite = pools.iterator();
				while(ite.hasNext()){
					
					ConnectionPool pool = ite.next();
					/*
					 * If current pool size exceeds minimum pool size and available sockets remain ,
					 * Fit to minimum size.   
					 */
					int current_size = pool.size();
					int result_size = pool.fit(KVSClient.POOL_MIN_SIZE);
					logger.debug("connection pool("+pool.getPoolName()+") sweeped ["+current_size+"->"+result_size+"]");
				}
			}
		}
		
		public void addConnectionPool(ConnectionPool pool){
			synchronized(pools){
				pools.add(pool);
			}
		}
		public void removeConnectionPool(ConnectionPool pool){
			synchronized(pools){
				pools.remove(pool);
			}
		}
		
		public int size(){
			if(pools==null){
				return 0;
			}
			return pools.size();
		}
		
		@Override
		public boolean cancel(){
			pools.clear();
			pools = null;
			return super.cancel();
		}
		
	}
	
	abstract class ConnectionPool{
		
		protected Queue<Integer> m_queue = null;
		protected Map<Integer ,Object> m_cache = null;
		protected InetSocketAddress address = null;
		
		public ConnectionPool(InetSocketAddress address){
			this.address = address;
		}
		
		public String getPoolName(){
			return getName();
		}
		abstract public void init() throws Exception;
		abstract public void remove(int id);
		public synchronized void clear(){
			
			if(m_queue!=null)
			{
				m_queue.clear();
				m_queue = null;
			}
			
			if(m_cache!=null)
			{
				Iterator<Integer> ite = m_cache.keySet().iterator();
				while(ite.hasNext())
				{
					remove(ite.next());	
				}
				m_cache.clear();
				m_cache = null;
			}
			
			notifyAll();
			
		}
		
		public int available(){
			if(m_queue!=null){
				return m_queue.size();
			}
			return 0;
		}
		
		public int size(){
			if(m_cache!=null){
				return m_cache.size();
			}
			return 0;
		}
		
		public synchronized int fit(int size){
			if(m_queue!=null&&m_cache!=null){
				while(m_queue.size()>0&&m_cache.size()>size)
				{
					Integer id = m_queue.poll();
					if(id!=null){
						remove(id);						
					}
				}
				notifyAll();
				return m_cache.size();
			}
			return 0;
		}
		
	}
	
	class SocketConnectionPool extends ConnectionPool{

		public SocketConnectionPool(InetSocketAddress address) throws Exception{
			super(address);
		}
		
		public synchronized void init() throws Exception{

			m_queue = new ConcurrentLinkedQueue<Integer>();
			m_cache = new ConcurrentHashMap<Integer,Object>(KVSClient.POOL_MAX_SIZE);
			
			if(this.address!=null){
				for(int i=0;i<KVSClient.POOL_INIT_SIZE;i++){
					createSocket();
				}
			}
		}
		
		public synchronized void remove(int id){
			if(m_cache!=null){
				try{
					Socket socket = (Socket)m_cache.remove(id);						
					socket.close();
					logger.debug("socket("+id+") removed.");
				}catch(Exception ignore){
				}
			}
		}
		
		private synchronized void createSocket()throws Exception{
			
			Socket socket = new Socket(address.getAddress(),address.getPort());
			socket.setTcpNoDelay(true);
			socket.setKeepAlive(true);
			socket.setSoTimeout(0);
			Integer id = socket.hashCode();			
			if(m_queue.offer(id)){
				m_cache.put(id, socket);
				logger.debug("socket("+id+") created.");
				notify();
			}
		}
		
		public Socket getSocket(){
			try{
				if(m_queue==null)
					throw new Exception("ConnectionPool is already finalized.");
				
				Integer id = m_queue.poll();
				if(id==null){
					if(m_cache.size()<KVSClient.POOL_MAX_SIZE)
					{
						createSocket();
					}else{
						synchronized(this){
							wait();
						}
					}
					return getSocket();
				}
				
				Socket socket = (Socket)m_cache.get(id);
				if(socket.isClosed()||!socket.isConnected()){
					m_cache.remove(socket.hashCode());
					return getSocket();
				}
				
				return socket; 
				
			}catch(InterruptedException e){
				logger.warn("ConnectionPool.getSocket("+Thread.currentThread().getId()+") is interrupted.");
			}catch(Exception e){
				logger.error(e);
			}
			return null;

		}
		
		public void releaseSocket(Socket socket){

			if(socket==null||m_queue==null){
				return;
			}
			
			if(m_queue.offer(socket.hashCode())){
				synchronized(this){
					notify();
				}
			}
		}
		

	}
	
}
