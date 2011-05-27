package jp.co.fujisan.lighthouse.client;

public class ClientException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8912137660499684357L;

	public ClientException(){
		super();
	}

	public ClientException(String msg){
		super(msg);
	}

	public ClientException(Exception e){
		super(e);
	}
	
	public ClientException(String msg,Exception e){
		super(msg,e);
	}
	
}
