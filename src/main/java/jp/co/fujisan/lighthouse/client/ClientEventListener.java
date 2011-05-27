package jp.co.fujisan.lighthouse.client;

public interface ClientEventListener {

	public void  availableEvent(Client client,boolean avaliable);
	public void  terminateEvent(Client client);
	public void  failEvent(Client client,Exception e);

}
