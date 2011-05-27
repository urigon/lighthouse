package jp.co.fujisan.lighthouse.queue.exception;

public class AlreadyFinalizedException extends Exception
{
	public AlreadyFinalizedException(){
		super();
	}

	public AlreadyFinalizedException(String msg) {
		super(msg);
	}
}
