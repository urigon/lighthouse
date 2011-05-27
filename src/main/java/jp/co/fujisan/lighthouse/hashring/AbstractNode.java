package jp.co.fujisan.lighthouse.hashring;

import jp.co.fujisan.lighthouse.client.Client;


public abstract class AbstractNode implements Node {

	private boolean m_available = false;
	
	protected long m_index = 0;
	protected String m_name;
	protected Client m_client;
	protected AbstractNode m_successor = null;
	protected AbstractNode m_predecessor = null;
	
	public AbstractNode(String name,Client client){
		m_name = name;
		m_client = client;
	}
	
	@Override
	public long getIndex(){
		return m_index;
	}
	
	protected void setIndex(long index){
		m_index = index;
	}
	
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return m_name;
	}

	@Override
	public Client getClient() {
		// TODO Auto-generated method stub
		return m_client;
	}

	@Override
	public void setClient(Client client) {
		// TODO Auto-generated method stub
		m_client = client;
	}

	@Override
	public boolean isAvailable(){
		return m_client.isAvailable(false);
	}
	
	protected void setAvailable(boolean isAvailable){
		m_available = isAvailable;
		m_client.isAvailable(false);
	}
	
//	protected void setSuccessor(AbstractNode successor) {
//		this.m_successor = successor;
//	}
//
//	protected final AbstractNode getSuccessor() {
//		return m_successor;
//	}
//
//	protected void setPredecessor(AbstractNode predecessor) {
//		this.m_predecessor = predecessor;
//	}
//
//	protected final AbstractNode getPredecessor() {
//		return m_predecessor;
//	}
	
	abstract protected AbstractNode clone();
}
