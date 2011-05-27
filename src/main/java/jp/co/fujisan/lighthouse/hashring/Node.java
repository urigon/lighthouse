package jp.co.fujisan.lighthouse.hashring;

import jp.co.fujisan.lighthouse.client.Client;

public interface Node {
	
	public long getIndex();
	
	public String getName();
	
	public Client getClient();
	
	public void setClient(Client client);
	
	public boolean isAvailable();
	
	public void replicate(Node dest_node);

}
