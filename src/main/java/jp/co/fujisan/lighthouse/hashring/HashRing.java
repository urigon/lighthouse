package jp.co.fujisan.lighthouse.hashring;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import jp.co.fujisan.lighthouse.client.Client;
import jp.co.fujisan.lighthouse.hashring.index.IndexGenerator;
import jp.co.fujisan.lighthouse.hashring.index.IndexGeneratorFactory;


/**
 * TODO Ringへの追加時にIDがコンフリクトした場合に、IDのLengthを変更してRingを伸ばす仕組みをついかする。
 * Index<Integer> -> Index<Double>
 * 
 * TODO Nodeの並列安全性についてだが、内部構造の変更については少なくともそれぞれ3つのノードリファレンスの同期さえ行えばいいのでは？
 * @author development
 *
 */
public class HashRing {
	
	private static final Log logger = LogFactory.getLog(HashRing.class);

	public final static long MIN_INDEX = Long.MIN_VALUE;
	public final static long MAX_INDEX = Long.MAX_VALUE;
	public final static long NULL_INDEX = 0;
	
	/**
	 * The renge of silce is 1 to 16
	 */
	//private static final int slice = 3;
	
	private static final String index_generator = IndexGeneratorFactory.GEN_TYPE_MD5; 
	
	private IndexGenerator indexGen = null;

	private TreeMap<Long,AbstractNode> sorted_map;
	private Map<Integer,Client> clients;
	
	private int totalWeight = 1;
	
	public HashRing(){
		sorted_map = new TreeMap<Long,AbstractNode>();
		clients = new HashMap<Integer,Client>();
		indexGen = IndexGeneratorFactory.getGenerator(index_generator);
	}

	public HashRing(List<Client> client_list){
		sorted_map = new TreeMap<Long,AbstractNode>();
		clients = new HashMap<Integer,Client>();
		indexGen = IndexGeneratorFactory.getGenerator(index_generator);
		
		this.generateCircle(client_list);
	}

	private void generateCircle(List<Client> client_list){
		
		if(client_list==null){
			return;
		}
		
		Iterator<Client> itr=client_list.iterator();
		while(itr.hasNext()){
			Client client = itr.next();
			if(client!=null){
				this.totalWeight += client.getWeight();
				clients.put(client.getId(), client);
			}
		}

		itr=client_list.iterator();
		while(itr.hasNext()){
			Client client = itr.next();
			if(client!=null){
				try{
					join(client);
				}catch(Exception e){
					logger.debug("<Error>\r\n",e);
				}
			}
		}
	}

	public int size(){
		return this.sorted_map.size();
	}
	
	public synchronized void clear(){
		
		try{
			/*
			 * とりあえず無効化
			 */
			if(this.size()>0){
				HashRingIterator ite = new HashRingIterator(this);
				while(ite.hasNext()){
					AbstractNode node = (AbstractNode)ite.next(); 
					if(node!=null){
						node.setAvailable(false);
					}
				}
				
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					logger.debug("<Error>\r\n",e);
				}
				
				ite = new HashRingIterator(this);
				while(ite.hasNext()){
					AbstractNode node = (AbstractNode)ite.next(); 
					if(node!=null){
						long index = node.getIndex();
						this.sorted_map.remove(index);
						this.removeFromRing(node);
					}
				}
			}
			
			this.sorted_map.clear();
			this.clients.clear();
			
		}catch(Exception e){
			logger.warn(e);
		}
		
	}
	
	/**
	 * HashRingへの追加
	 * 重み付けを行ってから、追加する。
	 * @param key
	 * @param node
	 * @throws Exception 
	 */
	public synchronized void join(Client client) throws Exception{
		
		if(!this.clients.containsKey(client.getId())){
			this.totalWeight += client.getWeight();
			this.clients.put(client.getId(), client);
		}
		
		/*
		 * 重み付けの計算
		 * TODO LightCloudから継承しているけど、ノードのコピー数はもう少し少なくてもよさそう。チューニングの必要あり。
		 */
		double factor = Math.floor( ((double)(40 * this.clients.size() * client.getWeight())) / (double)(this.totalWeight<0?1:this.totalWeight) );
		int clones = (int)factor*indexGen.getSlice();
		logger.debug("Clones="+clones);
		client.setCloneNumber(clones);

		long[] indexies = indexGen.genNodeIndex(client.getName()+":"+client.getId(),clones );
		for ( int i = 0; i < indexies.length; i++ ) {

			/*
			 * 重み付けに基づきインデックスを分散しノードを配置
			 */
			add(indexies[i],new SimpleNode(client.getName()+"-"+i,client));
			
		}
	}
	
	private void add(long index,Node node){
		
		AbstractNode you = (AbstractNode)node;

		try {
			if(this.sorted_map.containsKey(index)){
				throw new Exception("Node["+index+"] already exists");
			}
			/**
			 * Ringへの参加
			 */
			AbstractNode ref = addToRing(index,you);

			/**
			 * キャッシュへの追加
			 */
			this.sorted_map.put(index, ref);
			
			/**
			 * 有効化
			 */
			ref.setAvailable(true);
		
			//logger.debug("[HashRing] Add Node<"+node.getName()+":"+node.getClient().getId()+">  --> ["+index+"]");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.debug("<Error>\r\n",e);
			this.removeFromRing(you);
			this.sorted_map.remove(index);
		}
		
	}
	
	/**
	 * Ringへの追加
	 * nodeのcloneをRing上のindexの位置に追加する。
	 * @param index
	 * @param node
	 * @return
	 */
	private AbstractNode addToRing(long index,AbstractNode node){
		
		AbstractNode clone = node.clone();
		/*
		 *　自分のindexを設定。 
		 * */
		clone.setIndex(index);

		/*
		 * 与えられたindexから一番近い前方のノードを取得
		 **/
		AbstractNode predecessor = this.getPredecessor(index);
		AbstractNode successor = null;
		if(predecessor!=null){
			/*
			 * 自分だけしかまだRing上にいない
			 **/
			if(predecessor.getIndex() == index){
				return predecessor;
			}
			
			/*
			 * 前方ノードの後続ノードを取得する
			 * 前方ノードに有効な後続ノードが無い（既に削除されたか、離脱している）場合は
			 * キャッシュ上のIDから与えられたindexの次のindexにあるノードを取得する。
			 * */
			successor = predecessor.m_successor;
			if(successor==null){
				successor = this.getSuccessor(index);
			}
			
			/*
			 * 自分をRingに追加する。（前方ノードと後続ノードに割り込む）
			 * */
			if(successor!=null)
			{
				successor.m_predecessor = clone;
				clone.m_successor = successor;
			}
			predecessor.m_successor = clone;
			clone.m_predecessor = predecessor;

		}else{
			/*
			 * Ring上にノードがひとつも無い場合は、自分にリンクをはる。
			 **/
			clone.m_predecessor = clone;
			successor = this.getSuccessor(index);
			if(successor!=null)
			{
				successor.m_predecessor = clone;
				clone.m_successor = successor;
			}else{
				clone.m_successor = clone;
			}
		}
		
		return clone;
		
	}
	
	/**
	 * HashRingからクライアント単位のノードを削除する。
	 * 
	 * クライアントをダウンする際はこのメソッドでRing上から同じクライアントの全ノードを離脱させる。
	 * クライアントからのリクエストが既に離脱したノードを取得しようとした場合、一番近い後続のノードを検索し返す。
	 * @param key
	 * 
	 */
	public synchronized Client removeClient(Integer clientIndex){

		if(!this.sorted_map.isEmpty())
		{
			try {
				
				Client client = this.clients.get(clientIndex);
				if(client!=null){
					
					//重み付けを減算
					this.totalWeight -= client.getWeight();
					if(this.totalWeight<=0){
						this.totalWeight = 1;
					}
					
					/*
					 * clientIndexを元に分散されたインデックスの張られたノードを探し出す。
					 */
					long[] indexies = this.indexGen.genNodeIndex(client.getName()+":"+client.getId(), client.getCloneNumber());
					for(int i=0;i<indexies.length;i++){
						try{
							removeNode(indexies[i]);
						}catch(Exception e){
							logger.debug("<Error>\r\n",e);
						}
					}
					this.clients.remove(clientIndex);
					
				}
				return client;
			} catch (Exception e) {
				logger.debug("<Error>\r\n",e);
			}
		}
		return null;
		
	}
	
	/**
	 * HashRingからノードを削除する。
	 * 
	 * ノードをダウンする際はこのメソッドでRing上からノードを離脱させる。
	 * クライアントからのリクエストが既に離脱したノードを取得しようとした場合、一番近い後続のノードを検索し返す。
	 * @param key
	 * 
	 */
	public synchronized void removeNode(Long nodeIndex){

		if(!this.sorted_map.isEmpty())
		{
			try {
				
				if (nodeIndex>NULL_INDEX && this.sorted_map.containsKey(nodeIndex)) {

					AbstractNode node = this.sorted_map.remove(nodeIndex);
					if(node!=null){
						try{
							AbstractNode you = (AbstractNode)node;
							you.setAvailable(false);
						}catch(Exception e){
							logger.debug("<Error>\r\n",e);
						}finally{
							this.removeFromRing((AbstractNode)node);
							//logger.debug("[HashRing] Departed Node<"+node.getName()+":"+node.getClient().getId()+">  --> ["+index+"]");
							node = null;
						}
					}
				}
			} catch (Exception e) {
				logger.debug("<Error>\r\n",e);
			}
		}
		
	}
	/**
	 * Ringからの離脱
	 * 
	 * @param node
	 */
	private void removeFromRing(AbstractNode node){
		
		if(node == null){
			return;
		}
		
		/**
		 * 自身の知っている、前ノードと後ノードを取得
		 */
		AbstractNode predecessor = node.m_predecessor;
		AbstractNode successor = node.m_successor;
		
		/**
		 * 前ノードが見つからない場合（既に削除されているか、離脱している）
		 * 自分から一番近い前方のノードを見つける。
		 */
		if(predecessor==null){
			predecessor = this.getPredecessor(node.getIndex());
		}		
		
		/**
		 * 後ノードが見つからない場合（既に削除されているか、離脱している）
		 * 自分から一番近い後方のノードを見つける。
		 */
		if(successor==null){
			successor = this.getSuccessor(node.getIndex());
		}
		
		
		if(!node.equals(predecessor)&&!node.equals(successor)){
			/**
			 * 両方のノードが有効な場合のみ、リングをつなぐ
			 */
			if(predecessor!=null&&successor!=null){
				predecessor.m_successor = successor;
				successor.m_predecessor = predecessor;
			//}else if(predecessor!=null&&successor==null){
			//	predecessor.setSuccessor(null);
			//}else if(successor!=null&&predecessor==null){
			//	successor.setPredecessor(null);
			}
		}
		
		
		/**
		 * 自分はRingから離脱
		 */
		node.m_predecessor = null;
		node.m_successor = null;
		node = null;
	}

	 /**
	 * ノードの取得
	 * 検索順序
	 * 1. キャッシュ(sorted_map)からIndexでb-tree検索。
	 * 2. sorted_map上の一番近い後続ノードを返す。
	 * @param key
	 * @param isImplacable
	 * @return
	 */
	public Node getNode(String key ){

		Node node = null;
		
		if( !this.sorted_map.isEmpty())
		{
			try {
				long index = this.genIndex(key);
				
				node = this.getNode(index);
					
			} catch (Exception e) {
				logger.debug("<Error>\r\n",e);
			}
		}
		
		
		return node;
	}
	
	/**
	 * Indexによるノードの直接取得
	 * @param index
	 * @return
	 */
	public Node getNode(long index){
		
		Node node = null;
		
		if( !this.sorted_map.isEmpty())
		{
			try {
					
					/*
					 *　キャッシュから取得 
					 */
					if (index>NULL_INDEX && this.sorted_map.containsKey(index)) {
						node = this.sorted_map.get(index);
					}
					
					/*
					 * 次のIndexのノードを返す。
					 * */
					if(node==null){
						node = this.getSuccessor(index);
					}
				
			} catch (Exception e) {
				logger.debug("<Error>\r\n",e);
			}
		}
		
		return node;
	}
	
	/**
	 * 有効な先方ノードを取得する
	 * @param index
	 * @return
	 */
	protected AbstractNode getPredecessor(long index){
		
		if(!this.sorted_map.isEmpty()){
			try {
				/*
				 * パラメーターより小さいインデックスのエントリーを取得
				 */
				Entry<Long, AbstractNode> entry = this.sorted_map.lowerEntry(index);
				if(entry!=null){
					return entry.getValue();
				}
				/*
				 * エントリが無い=パラメータが最初のインデックスかそれより小さい場合、
				 * 一番最後のインデックスのエントリーを取得
				 */
				entry = this.sorted_map.lastEntry();
				if(entry!=null){
					return entry.getValue();
				}
			} catch (Exception e) {
				logger.debug("<Error>\r\n",e);
			}
			
		}		
		return null;
		
	}

	/**
	 * 与えられたidの直前にあるインデックスを取得する。
	 * @param index
	 * @return　直前のインデックス、無い場合0
	 */
	protected long getPredecessorIndex(long index){
		
		if(!this.sorted_map.isEmpty()){
			try {
				/*
				 * パラメーターより小さいインデックスを取得
				 */
				Long key = this.sorted_map.lowerKey(index);
				if(key== null){
					/*
					 * インデックスが無い=パラメータが最初のインデックスかそれより小さい場合、
					 * 一番最後のインデックスを取得
					 */
					key = this.sorted_map.lastKey();
				}
				return key;
			} catch (Exception e) {
			}
		}
		return  NULL_INDEX;
		
	}
	
	/**
	 * 有効な後方ノードを取得する
	 * @param index
	 * @return
	 */
	protected AbstractNode getSuccessor(long index){
		
		if(!this.sorted_map.isEmpty()){
			try {
				/*
				 * パラメーターより大きいインデックスのエントリーを取得
				 */
				Entry<Long, AbstractNode> entry = this.sorted_map.higherEntry(index);
				if(entry!=null){
					return entry.getValue();
				}
				/*
				 * エントリが無い=パラメータが最後のインデックスかそれより大きい場合、
				 * 一番最初のインデックスのエントリーを取得
				 */
				entry = this.sorted_map.firstEntry();
				if(entry!=null){
					return entry.getValue();
				}
			} catch (Exception e) {
				logger.debug("<Error>\r\n",e);
			}
			
		}
		return null;
		
	}
	
	/**
	 * 与えられたidの直後にあるインデックスを取得する。
	 * @param index
	 * @return　直後のインデックス、無い場合0
	 */
	protected long getSuccessorIndex(long index){
		if(!this.sorted_map.isEmpty()){
			try {
				/*
				 * パラメーターより大きいインデックスを取得
				 */
				Long key = this.sorted_map.higherKey(index);
				if(key== null){
					/*
					 * インデックスが無い=パラメータが最後のインデックスかそれより大きい場合、
					 * 一番最初のインデックスを取得
					 */
					key = this.sorted_map.firstKey();
				}
				return key;
			} catch (Exception e) {
			}
		}
		return  NULL_INDEX;
	}
	
	/**
	 * 最初のインデックスを取得する。
	 * @return　最初のインデックス
	 */
	protected long getFirstIndex(){
		if(!this.sorted_map.isEmpty()){
			return this.sorted_map.firstKey();
		}
		return NULL_INDEX;
	}
	
	/**
	 * 最後のインデックスを取得する。
	 * @return　最後のインデックス
	 */
	protected long getLastIndex(){
		if(!this.sorted_map.isEmpty()){
			return this.sorted_map.lastKey();
		}
		return NULL_INDEX;
	}
	
	/**
	 * Node.nameからIndexが生成される。
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public long genIndex(String key){
		try {
			return indexGen.genIndex(key);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.debug("<Error>\r\n",e);
		}
		return NULL_INDEX;
	}
	
	
	public synchronized void dump(){
		this.dump("",3);
	}
	
	public synchronized void dump(String title){
		this.dump(title,3);
	}
	public synchronized void dump(String title,int option){
		if(logger.isDebugEnabled()){
			boolean isPrintSortedMap = true;
			boolean isPrintRing = true;
			switch(option){
			case -1:
				return;
			case 1:
				isPrintSortedMap = true;
				isPrintRing = false;
				break;
			case 2:
				isPrintSortedMap = false;
				isPrintRing = true;
				break;
			default:
				isPrintSortedMap = true;
				isPrintRing = true;
			}
			
			if(title!=null&&title.length()>0){
				title = "["+title+"]";
			}else{
				title = "";
			}
			try{
				System.out.println("=====DUMPPING HASH_RING "+title+"==============");
				if(isPrintSortedMap){
					System.out.println("=====<HashRing("+this.hashCode()+").sorted_map>=====");
					for(Iterator<Long> itr=this.sorted_map.keySet().iterator(); itr.hasNext();){
						Long index = itr.next();
						Node node = this.sorted_map.get(index);
						if(node == null){
							System.out.println("     ["+index+"]=null");
						}else{
							System.out.println("     ["+index+"]="+node.getName());
						}
					}
					System.out.println();
				}
				
				if(isPrintRing){
					System.out.println("=====<HashRing("+this.hashCode()+").RING>=====");
					AbstractNode r_node = this.getPredecessor(0);	
					if(r_node == null){
						System.out.println("Ring is empty.");
						return;
					}
					long lastKey = r_node.getIndex();
					long curKey = 0;
					r_node = r_node.m_successor;
					while(r_node!=null&&curKey!=lastKey)
					{
						curKey = r_node.getIndex();
						String curName = r_node.getName();
						long sucIndex  = 0;
						r_node = r_node.m_successor;
						if(r_node == null){
							break;
						}
						sucIndex = r_node.getIndex();
						System.out.println("     ["+curKey+"]"+curName+"-->["+sucIndex+"]");
					}
				}
				
			}catch(Exception e){
				logger.debug("<Error>\r\n",e);
			}
		}
	}
	
}
