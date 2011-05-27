package jp.co.fujisan.lighthouse.client.exception;

import java.io.File;

import org.apache.commons.configuration.XMLConfiguration;

public class ClientConfigurationException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1764977546540441128L;
	XMLConfiguration config_file = null;
	String message = "";
	public ClientConfigurationException(){
		super();
	}

	public ClientConfigurationException(String msg){
		super();
		this.message = msg;
	}

	public ClientConfigurationException(Exception e){
		super();
		this.message =  "Caused by ["+e.toString()+"] " + e.getMessage();
	}
	
	public ClientConfigurationException(String msg,Exception e){
		super();
		this.message =  msg + " Caused by ["+e.toString()+"] " + e.getMessage();
	}
	
	public ClientConfigurationException(XMLConfiguration config,String msg){
		super();
		config_file = config;
		this.message =  msg + " : configuration file is ["+(config==null?"unknown":config.getBasePath())+File.separator+(config==null?"unknown":config.getFileName())+"]";
	}
	
	public String getMessage(){
		return this.message;
	}
}
