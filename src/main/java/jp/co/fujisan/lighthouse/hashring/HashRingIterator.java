package jp.co.fujisan.lighthouse.hashring;

import java.util.Iterator;

public class HashRingIterator implements Iterator<Node>{

	HashRing m_ring = null;
	AbstractNode curNode = null;
	private long start_index = HashRing.NULL_INDEX;
	private long end_index = HashRing.NULL_INDEX;
	
	private volatile boolean untouched = true;
	/**
	 * 最初のインデックス位置からのHashRingIterator
	 * 最後のインデックスで終了します。
	 * @param ring
	 * @param position
	 * @throws NothingNodeException 
	 */
	public HashRingIterator(HashRing ring) throws NothingNodeException{
		if(ring==null||ring.size()==0){
			throw new NothingNodeException();
		}
		m_ring = ring;
		curNode = (AbstractNode)m_ring.getPredecessor(m_ring.getFirstIndex());
		untouched = true;
		if(curNode!=null){
			this.end_index = curNode.getIndex();
			if(curNode.m_successor!=null){
				this.start_index = curNode.m_successor.m_index;
			}else if(m_ring.getSuccessor(this.end_index)!=null){
				this.start_index = m_ring.getSuccessor(this.end_index).m_index;
			}else{
				this.start_index = this.end_index;
			}
		}
	}

	/**
	 * 指定のインデックス位置からのHashRingIterator
	 * 指定したインデックスの手前で終了します。
	 * @param ring
	 * @param position
	 * @throws NothingNodeException 
	 */
	public HashRingIterator(HashRing ring,long position) throws NothingNodeException{
		if(ring==null||ring.size()==0){
			throw new NothingNodeException();
		}
		m_ring = ring;
		curNode = (AbstractNode)m_ring.getPredecessor(position);
		untouched = true;
		if(curNode!=null){
			this.end_index = curNode.getIndex();
			if(curNode.m_successor!=null){
				this.start_index = curNode.m_successor.m_index;
			}else if(m_ring.getSuccessor(this.end_index)!=null){
				this.start_index = m_ring.getSuccessor(this.end_index).m_index;
			}else{
				this.start_index = this.end_index;
			}
		}
	}
	
	/**
	 * 指定のキーのノードのインデックス位置からのHashRingIterator
	 * 指定したインデックスの手前で終了します。
	 * @param ring
	 * @param position
	 * @throws NothingNodeException 
	 */
	public HashRingIterator(HashRing ring,String key) throws NothingNodeException{
		if(ring==null||ring.size()==0){
			throw new NothingNodeException();
		}
		m_ring = ring;
		AbstractNode node = (AbstractNode)m_ring.getNode(key);
		curNode = node.m_predecessor;
		untouched = true;
		if(curNode!=null){
			this.end_index = curNode.getIndex();
			if(curNode.m_successor!=null){
				this.start_index = curNode.m_successor.m_index;
			}else if(m_ring.getSuccessor(this.end_index)!=null){
				this.start_index = m_ring.getSuccessor(this.end_index).m_index;
			}else{
				this.start_index = this.end_index;
			}
		}
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if(curNode!=null)
		{
			//後続ノードが存在しているか？
			if(curNode.m_successor==null&&m_ring.getSuccessor(curNode.getIndex())==null){
				//RingにNodeがひとつも無い
				return false;
			}
			
			/*
			 * 常に最新のインデックスを取得する。
			 */
			Node firstNode = m_ring.getNode(this.end_index);
			if(firstNode == null){
				//RingにNodeがひとつも無い
				return false;
			}else{
				long new_end_index = firstNode.getIndex();
				if(this.end_index != new_end_index){
					// インスタンス生成時の最終ノードの離脱によってend_indexが変わってしまう場合
					
					if(untouched){
						this.end_index = new_end_index;
					}
					/*
					 * new_end_indexは現在位置(curNode.m_index)を追い越さない為、
					 * (追い越す場合は、curNodeが既にRingから離脱している状態のみ)
					 * end_indexが現在位置(curNode.m_index)になってしまう場合だけ、
					 * 前回最終ノードから一つ前のノードを最終ノードとする。
					 */
					if(curNode.m_index == new_end_index){
						this.end_index = m_ring.getPredecessor(this.end_index).m_index;
					}else{
						/*
						 * 以外の場合は、ノードがend_index以降に追加されたと考えて
						 * 最終ノード位置を更新する。
						 */
						this.end_index = new_end_index;
					}
				}
			}
			
			if(untouched){
				//インスタンス生成直後でカーソルがend_indexになっているため、初回はOK
				return true;
			}
			
			if(curNode.m_index != this.end_index){
				//まだRingの終端まできていないのでOK
				return true;
			}
		}
		return false;
	}

	@Override
	public Node next() {
		
		if(curNode!=null)
		{
			AbstractNode next_node = null;
			
			//次のノードを取得
			if(curNode.m_successor==null){
				next_node = m_ring.getSuccessor(curNode.getIndex());
			}else{
				next_node = curNode.m_successor;
			}
			
			if(untouched){
				untouched = false;
				curNode = next_node;
				return curNode;
			}

			/*
			 * 常に最新のインデックスを取得する。
			 */
			Node firstNode = m_ring.getNode(this.end_index);
			if(firstNode == null){
				//RingにNodeがひとつも無い
				curNode = null;
				return null;
			}else{
				long new_end_index = firstNode.getIndex();
				if(this.end_index != new_end_index){
					// インスタンス生成時の最終ノードの離脱によってend_indexが変わってしまう場合
					
					/*
					 * new_end_indexは現在位置(curNode.m_index)を追い越さない為、
					 * (追い越す場合は、curNodeが既にRingから離脱している状態のみ)
					 * end_indexが現在位置(curNode.m_index)になってしまう場合だけ、
					 * 前回最終ノードから一つ前のノードを最終ノードとする。
					 */
					if(curNode.m_index == new_end_index){
						this.end_index = m_ring.getPredecessor(this.end_index).m_index;
					}else{
						/*
						 * 以外の場合は、ノードがend_index以降に追加されたと考えて
						 * 最終ノード位置を更新する。
						 */
						this.end_index = new_end_index;
					}
				}
			}
			
			if(curNode.m_index != this.end_index){
				//まだRingの終端まできていないのでOK
				curNode = next_node;
				return curNode;
			}
		}
		return null;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

}
