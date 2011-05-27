package jp.co.fujisan.lighthouse.hashring;

public class NothingNodeException extends Exception {

	public NothingNodeException(){
		super("Nothing node exists on HashRing.");
	}
}
