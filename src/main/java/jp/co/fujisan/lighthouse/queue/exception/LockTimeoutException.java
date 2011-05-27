package jp.co.fujisan.lighthouse.queue.exception;

public class LockTimeoutException extends Exception
{
	public LockTimeoutException(){
		super();
	}
	public LockTimeoutException(String msg){
		super(msg);
	}
}
