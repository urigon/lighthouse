package jp.co.fujisan.lighthouse.lock;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import jp.co.fujisan.lighthouse.LightHouse;
import jp.co.fujisan.lighthouse.client.Client;
import jp.co.fujisan.lighthouse.client.ClientEventListener;
import jp.co.fujisan.lighthouse.client.KVSClient;
import jp.co.fujisan.lighthouse.client.exception.UnRecoverableException;
import jp.co.fujisan.lighthouse.queue.exception.LockFailureException;
import jp.co.fujisan.lighthouse.queue.exception.LockTimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

public class GlobalLockClientManager implements Serializable, InitializingBean, DisposableBean{
	
	private static final Log logger = LogFactory.getLog(GlobalLockClientManager.class);
	
	public final static String DELIMITTER_Q = "?";
	public final static String DELIMITTER_AMP = "&";
	public final static String DELIMITTER_EQ = "=";
	public final static String PARAM_KEY = "key";
	public final static String PARAM_GROUP = "group";
	public final static String PARAM_ID = "id";
	
	private final static String CMD_PREFIX_LOCK = GlobalLockServer.CMD_LOCK+DELIMITTER_Q+PARAM_KEY+DELIMITTER_EQ;
	private final static String CMD_PREFIX_UNLOCK = GlobalLockServer.CMD_UNLOCK+DELIMITTER_Q+PARAM_KEY+DELIMITTER_EQ;

	InetSocketAddress[] remoteAddresses = null;
	GlobalLockClient[] m_clients = null;
	
	public long lock(long token,String key)throws LockTimeoutException,LockFailureException{
		return lock(token,null,key);
	}
	public long lock(long token, String group,String key)throws LockTimeoutException,LockFailureException{
		for(int i=0;i<m_clients.length;i++){
			GlobalLockClient client = m_clients[i];
			if(client!=null){
				try {
					long result = client.lock(token,group,key);
					//他のスレッドがロックを取得している
					if(token!=result){
						//Rollback
						for(;i>=0;i--){
							try{
								GlobalLockClient rb_client = m_clients[i];
								if(rb_client!=null){
									rb_client.unlock(token,group, key);
								}
							}catch(Exception ignore){
								
							}
						}
						throw new LockFailureException("Failed to lock on node "+client.host+":"+client.port);						
					}
				}catch(LockTimeoutException e){
					throw e;
				}catch(LockFailureException e){
					throw e;
				} catch (Exception e) {
					logger.error(e);
				}
			}
		}
		return token;
	}
	public long unlock(long token,String key)throws LockTimeoutException,LockFailureException{
		return unlock(token,null,key);
	}
	public long unlock(long token,String group,String key)throws LockTimeoutException,LockFailureException{
		for(int i=0;i<m_clients.length;i++){
			GlobalLockClient client = m_clients[i];
			if(client!=null){
				try{
					long result = client.unlock(token,group,key);
					if(token!=result){
						throw new LockFailureException("Failed to unlock on node "+client.host+":"+client.port);						
					}
				}catch(LockTimeoutException e){
					throw e;
				}catch(LockFailureException e){
					throw e;
				} catch (Exception e) {
					logger.error(e);
				}
			}
		}
		return token;
	}
	
	public GlobalLockClientManager()throws Exception{
		
	}
	
	public GlobalLockClientManager(InetSocketAddress[] remote_addresses)throws Exception{
		setRemoteAddresses(remote_addresses);
	}
	
	public void setRemoteAddresses(InetSocketAddress[] remote_addresses){
		remoteAddresses = remote_addresses;
		List<GlobalLockClient> list = new ArrayList<GlobalLockClient>();
		for(int i=0;i<remote_addresses.length;i++){
			try{
				GlobalLockClient client = new GlobalLockClient(remote_addresses[i].getHostName(),remote_addresses[i].getPort() );
				list.add(client);
			}catch(Exception e){
				logger.error(e);
			}
		}
		m_clients = list.toArray(new GlobalLockClient[list.size()]);
	}
	
	public void startup(){
//		for(int i=0;i<m_clients.length;i++){
//			GlobalLockClient client = m_clients[i];
//		}
	}
	
	@Override
	public void destroy() throws Exception {
//		for(int i=0;i<m_clients.length;i++){
//			try{
//				GlobalLockClient client = m_clients[i];
//			}catch(Exception e){
//				logger.warn(e);
//			}
//			m_clients[i] = null;
//		}
//		m_clients = null;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	class GlobalLockClient{

		String host = null;
		int port = 1976;
		public GlobalLockClient(String host, int host_port) throws Exception {
			this.host = host;
			this.port =  host_port;
			// TODO Auto-generated constructor stub
		}

		public long lock(long id,String group,String key)throws Exception,LockTimeoutException,LockFailureException{
			if(group!=null&&group.length()>0){
				return execute(id,CMD_PREFIX_LOCK+key+DELIMITTER_AMP+PARAM_GROUP+DELIMITTER_EQ+group);
			}else{
				return execute(id,CMD_PREFIX_LOCK+key);
			}
		}
		public long unlock(long id,String group,String key)throws Exception,LockTimeoutException,LockFailureException{
			if(group!=null&&group.length()>0){
				return execute(id,CMD_PREFIX_UNLOCK+key+DELIMITTER_AMP+PARAM_GROUP+DELIMITTER_EQ+group);
			}else{
				return execute(id,CMD_PREFIX_UNLOCK+key);
			}
		}
		
		private long execute(long id,String cmd)throws Exception{
			
			Socket socket = null; 
			try {
				socket = new Socket(host,port); 
				OutputStream out = new BufferedOutputStream( socket.getOutputStream());
				cmd += DELIMITTER_AMP+PARAM_ID+DELIMITTER_EQ+id;
				out.write( cmd.getBytes() );
				out.write( "\r\n".getBytes() );
				out.flush();
				
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String ret = in.readLine();
				if(ret!=null){
					if(logger.isDebugEnabled()){
						logger.debug(cmd +"=>"+ ret);
					}
					if ( GlobalLockServer.RES_TIME_OUT.equals( ret ) ) {
						throw new LockTimeoutException("Timed out to lock on remote node ["+super.toString()+"].");
					}else if ( GlobalLockServer.RES_ERROR.equals( ret ) ) {
						throw new LockFailureException("Failed to lock on remote node ["+super.toString()+"].");
					}
					return Integer.parseInt(ret);
				}
				
				throw new LockFailureException("Failed to lock on remote node ["+super.toString()+"].");
				
			} catch (LockTimeoutException e) {
				throw e;
			} catch (LockFailureException e) {
				throw e;
			} catch (Exception e) {
				throw e;
			}finally{
				if(socket!=null){
					try{
						socket.close();
					}catch(IOException e){
					}
				}
			}
		}
		
	}

}
