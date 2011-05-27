package jp.co.fujisan.lighthouse.exception;

public class StatusException extends Exception {
	int status_code = 0;
	public StatusException(int status_code ,String msg){
		super(msg);
		this.status_code = status_code;
	}
	
	public int getStatusCode(){
		return status_code;
	}
}
