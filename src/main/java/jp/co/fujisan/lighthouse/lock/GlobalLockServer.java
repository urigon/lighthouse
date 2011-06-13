package jp.co.fujisan.lighthouse.lock;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jp.co.fujisan.lighthouse.queue.KVQueueLockingImpl;
import jp.co.fujisan.lighthouse.queue.LockingConcurrentHashMap;
import jp.co.fujisan.lighthouse.queue.exception.LockFailureException;
import jp.co.fujisan.lighthouse.queue.exception.LockTimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

public class GlobalLockServer implements Serializable, InitializingBean, DisposableBean{

	private static final Log logger = LogFactory.getLog(GlobalLockServer.class);

	public final static String CMD_LOCK = "lock";
	public final static String CMD_UNLOCK = "unlock";
	//public final static String RES_OK = "ok";
	public final static String RES_TIME_OUT = "timeout";
	public final static String RES_ERROR = "error";
	
	private final static Pattern ptrn_lock = Pattern.compile(
			"^"+CMD_LOCK+"\\"+GlobalLockClientManager.DELIMITTER_Q+
			GlobalLockClientManager.PARAM_KEY+GlobalLockClientManager.DELIMITTER_EQ+"(.*)"+
			"\\"+GlobalLockClientManager.DELIMITTER_AMP+GlobalLockClientManager.PARAM_ID+GlobalLockClientManager.DELIMITTER_EQ+"(.*)$");
	private final static Pattern ptrn_lock_group = Pattern.compile(
			"^"+CMD_LOCK+"\\"+GlobalLockClientManager.DELIMITTER_Q+
			GlobalLockClientManager.PARAM_KEY+GlobalLockClientManager.DELIMITTER_EQ+"(.*)"+
			"\\"+GlobalLockClientManager.DELIMITTER_AMP+GlobalLockClientManager.PARAM_GROUP+GlobalLockClientManager.DELIMITTER_EQ+"(.*)"+
			"\\"+GlobalLockClientManager.DELIMITTER_AMP+GlobalLockClientManager.PARAM_ID+GlobalLockClientManager.DELIMITTER_EQ+"(.*)$");
	private final static Pattern ptrn_unlock = Pattern.compile(
			"^"+CMD_UNLOCK+"\\"+GlobalLockClientManager.DELIMITTER_Q+
			GlobalLockClientManager.PARAM_KEY+GlobalLockClientManager.DELIMITTER_EQ+"(.*)"+
			"\\"+GlobalLockClientManager.DELIMITTER_AMP+GlobalLockClientManager.PARAM_ID+GlobalLockClientManager.DELIMITTER_EQ+"(.*)$");
	private final static Pattern ptrn_unlock_group = Pattern.compile(
			"^"+CMD_UNLOCK+"\\"+GlobalLockClientManager.DELIMITTER_Q+
			GlobalLockClientManager.PARAM_KEY+GlobalLockClientManager.DELIMITTER_EQ+"(.*)"+
			"\\"+GlobalLockClientManager.DELIMITTER_AMP+GlobalLockClientManager.PARAM_GROUP+GlobalLockClientManager.DELIMITTER_EQ+"(.*)"+
			"\\"+GlobalLockClientManager.DELIMITTER_AMP+GlobalLockClientManager.PARAM_ID+GlobalLockClientManager.DELIMITTER_EQ+"(.*)$");

	private ExecutorService m_executor_service = null;
	private ServerThread m_server_thread = null; 
	private static volatile ConcurrentHashMap<String,LockExpiration> m_locks = new ConcurrentHashMap<String,LockExpiration>();
	private static Timer lock_expire_timer=  null;

	private long requestTimeout = 10000;
	public void setRequestTimeout(long requestTimeout) {
		this.requestTimeout = requestTimeout;
	}
	private long exipreTimeout = 30000;
	public void setExipreTimeout(long exipreTimeout) {
		this.exipreTimeout = exipreTimeout;
	}
	private String host = "localhost";
	public void setHost(String host) {
		this.host = host;
	}
	private int port = 1976;
	public void setPort(int port) {
		this.port = port;
	}

	
	public GlobalLockServer() throws IOException{
		lock_expire_timer = new Timer("LockExpirationTimer");

	}
	
	public GlobalLockServer(InetSocketAddress endpoint,long lock_timeout_millis) throws IOException{
		setHost(endpoint.getHostName());
		setPort(endpoint.getPort());
		lock_expire_timer = new Timer("LockExpirationTimer");
		
		this.exipreTimeout = lock_timeout_millis;
	}
	
	public void startup() throws IOException,BindException{
		m_executor_service = Executors.newCachedThreadPool();
		InetSocketAddress endpoint = new InetSocketAddress(host,port);
		m_server_thread = new ServerThread(endpoint);
		m_executor_service.execute(m_server_thread);
	}
	
	@Override
	public void destroy() throws Exception {
		if(m_server_thread!=null){
			m_server_thread.exit();
		}
		if(m_executor_service!=null){
			m_executor_service.shutdownNow();
		}
		if(lock_expire_timer!=null){
			lock_expire_timer.cancel();
			lock_expire_timer.purge();
			lock_expire_timer = null;
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	private class ServerThread extends Thread{
		
		volatile boolean is_run = false;
		ServerSocket m_server = null;
		
		public ServerThread(InetSocketAddress endpoint) throws IOException{
			m_server = new ServerSocket(endpoint.getPort(),100,endpoint.getAddress());
			is_run = true;
		}
		
		@Override
		public void run(){
			while(is_run){
				try {
					m_executor_service.execute(new ProcessThread(m_server.accept()));
				} catch (Exception e) {
					logger.error("Error on creation processing lock thread." + e);
				}
			}
		}
		
		public void exit() throws IOException{
			is_run = false;
			m_server.close();
		}

	}
	
	class LockExpiration extends TimerTask
	{

		private String key = null;
		private long id = 0;
		public LockExpiration(long id, String key){
			this.id = id;
			this.key = key;
		}
		
		public long getId(){
			return this.id;
		}
		
		@Override
		public void run() {
			//自身をロックプールから削除
			LockExpiration exp = m_locks.remove(key);
			if(logger.isDebugEnabled()&&exp!=null){
				logger.debug("Lock ["+key+"] expired !");
			}
		}
		
		@Override
		public boolean cancel(){
			LockExpiration exp = m_locks.remove(key);
			if(logger.isDebugEnabled()&&exp!=null){
				logger.debug("Lock ["+key+"] canceled !");
			}
			return super.cancel();
		}
	}

	class ProcessThread extends Thread
	{
		private Socket socket = null;
		public ProcessThread(Socket socket){
			this.socket = socket;
		}
		
		@Override
		public void run(){
			
			OutputStream out = null;
			try {
				if(logger.isDebugEnabled())
					logger.debug("connected from "+ socket.getRemoteSocketAddress() );					
				
				out = new BufferedOutputStream( socket.getOutputStream());
				
				//Receive request
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String request = in.readLine();
				if(request!=null){
					
					if(logger.isDebugEnabled())
						logger.debug("request [ " + request+" ]");
					String token = null;
					
					long lock_id = 0;
					if(request.startsWith(CMD_LOCK)){
						Matcher m = ptrn_lock_group.matcher(request);
						String id = null;
						if(m.find()){
							String key = m.group(1);
							String group = m.group(2);
							if(key!=null&&group!=null){
								token = group+":"+key; 
							}
							id = m.group(3);
						}else{
							m = ptrn_lock.matcher(request);
							if(m.find()){
								String key = m.group(1);
								token = key; 
								id = m.group(2);
							}
						}
						if(id!=null&&id.length()>0){
							try{
								lock_id = Long.parseLong(id);
							}catch(Exception e){
								
							}
						}
						
						/*
						 * LOCK
						 */
						if(logger.isDebugEnabled())
							logger.debug("attempt to lock with token = " + token);
						try{
							long remains_to_timeout = requestTimeout;
							while(remains_to_timeout>0){
								synchronized(this){
									LockExpiration expire = null;
									synchronized(m_locks){
										if(!m_locks.containsKey(token)){
											expire = new LockExpiration(lock_id,token);
											m_locks.put(token,expire );
										}
									}
									if(expire!=null){
										lock_expire_timer.schedule(expire, exipreTimeout);
										if(logger.isDebugEnabled())
											logger.debug("locked ("+expire.getId()+"):"+expire.scheduledExecutionTime()+"ms with token = " + token);
										out.write( String.valueOf(expire.getId()).getBytes() );
										return;
									}else{
										sleep(10);
										remains_to_timeout-=10;
									}
								}
							}
							
							if(remains_to_timeout!=requestTimeout){
								if(logger.isDebugEnabled())
									logger.debug("lock timedout with token = " + token);
								out.write( RES_TIME_OUT.getBytes() );
							}
							
						}catch(Exception e){
							out.write( RES_ERROR.getBytes() );
							logger.warn(e);
						}
						
					}else if(request.startsWith(CMD_UNLOCK)){
						Matcher m = ptrn_unlock_group.matcher(request);
						String id = null;
						if(m.find()){
							String key = m.group(1);
							String group = m.group(2);
							if(key!=null&&group!=null){
								token = group+":"+key; 
							}
							id = m.group(3);
						}else{
							m = ptrn_unlock.matcher(request);
							if(m.find()){
								String key = m.group(1);
								token = key; 
								id = m.group(2);
							}
						}
						if(id!=null&&id.length()>0){
							try{
								lock_id = Long.parseLong(id);
							}catch(Exception e){
							}
						}
						
						/*
						 * UNLOCK
						 */
						if(logger.isDebugEnabled())
							logger.debug("attempt to unlock with token = " + token);
						try{
							LockExpiration expire = m_locks.get(token);
							if(expire!=null){
								if(expire.getId()==lock_id){
									expire.cancel();
									if(logger.isDebugEnabled())
										logger.debug("unlocked ("+expire.getId()+") with token = " + token);
								}else{
									if(logger.isDebugEnabled())
										logger.debug("unlock does not performed ("+expire.getId()+") with token = " + token);
									
								}
								out.write( String.valueOf(expire.getId()).getBytes() );
								//lock_expire_timer.purge();
								expire = null;
							}else{
								out.write( String.valueOf(0).getBytes() );
							}
						}catch(Exception e){
							out.write( RES_ERROR.getBytes() );
							logger.warn(e);
						}

					}
				}
			} catch (Exception e) {
				logger.error("Error on processing request to lock." + e);
			}finally{
				try {
					if(out!=null){
						out.write( "\r\n".getBytes() );
						out.flush();
						out.close();
					}
					if(socket!=null){
						socket.close();
					}
				} catch (IOException e1) {
					logger.error("Error on processing request to lock." + e1);
				}
			}
		}
		
	}
}
