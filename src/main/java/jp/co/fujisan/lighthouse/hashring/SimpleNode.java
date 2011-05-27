package jp.co.fujisan.lighthouse.hashring;

import jp.co.fujisan.lighthouse.client.Client;


public class SimpleNode extends AbstractNode {

	public SimpleNode(String name, Client client) {
		super(name, client);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void replicate(Node dest_node){
		/**
		 * Copy entries to 
		 * this.m_server => dest_node.getServer();
		 */
	}
	
	protected AbstractNode clone() {
		// TODO Auto-generated method stub
		AbstractNode clone = new SimpleNode(this.m_name,(Client)this.m_client);
		return clone;
	}
	
}
