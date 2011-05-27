package jp.co.fujisan.lighthouse.queue.exception;

public class LockFailureException extends Exception
{
	public LockFailureException(){
		super();
	}
	public LockFailureException(String msg){
		super(msg);
	}
}
