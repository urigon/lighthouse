package jp.co.fujisan.lighthouse.hashring

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List
import java.util.Map;
import java.util.Random;
import java.util.Set;

import jp.co.fujisan.lighthouse.client.Client
import jp.co.fujisan.lighthouse.client.ClientEventListener;
import jp.co.fujisan.lighthouse.client.ClientException;
import static org.junit.Assert.*
import org.gmock.WithGMock
import org.junit.runner.RunWith
import org.springframework.util.StopWatch;

import org.spockframework.runtime.Sputnik
import spock.lang.*

@WithGMock
@RunWith(Sputnik)
class HashRingSpec extends Specification{
    List<Client> client_list = [
            new TestServer("Server_name_1",1),
            new TestServer("Server_name_2",1),
            new TestServer("Server_name_3",1),
            new TestServer("Server_name_4",1),
            new TestServer("Server_name_5",1)
        ]
    StopWatch stopWatch = new StopWatch()
    def vervose = true

    def setup(){
        stopWatch.start()
    }
    def cleanup(){
        stopWatch.stop()
        println stopWatch.prettyPrint()
    }
    
    def "HashRingは指定されたクライアントのリストのハッシュリングを生成する"(){
        when:
        HashRing ring = new HashRing(client_list)
        
        then:
        ring.clients.size() == 5
        ring.totalWeight == 6
        ring.sorted_map.size() == 660
    }
    
    def "join は指定されるclientをHashRingに追加をする"(){
        given:
        HashRing ring = new HashRing()
        def client = new TestServer("Server_name_1",1)
        
        when:
        ring.join(client)
        
        then: "クライアント数とTotalWeightがインクリメントされる。"
        ring.clients.size() == 1
        ring.totalWeight == 2
        
        then: "また、クローンされた数のHashRingのNodeが add() されている"
        client.getCloneNumber() == ring.sorted_map.size()
    }
    
    def "add はリングに何も存在しない場合にNodeを追加するとそのNode自身がSuccessorとPredecessorとなる"(){
        given:
        def index = 1
        HashRing ring = new HashRing()
        
        when:
        ring.add(index, new SimpleNode("NodeName", new TestServer("Server_name_1", 1)))
        
        then:
        ring.sorted_map.size() == 1
        ring.getSuccessor(1).index == index
        ring.getPredecessor(1).index == index
    }
    
    def "add はリングに指定するIndexがすでに存在するとそのIndexに存在するものを削除する"(){
        given:
        def index = 1
        HashRing ring = new HashRing()
        
        when:
        ring.add(index, new SimpleNode("NodeName", new TestServer("Server_name_1", 1)))
        ring.add(index, new SimpleNode("NodeName", new TestServer("Server_name_2", 1)))
        
        then:
        ring.sorted_map.size() == 0
    }
    
    def "add は 既にIndexの値が指定するIndexより低いNodeがHashRingに存在し、"(){
        given:
        def index = 1
        HashRing ring = new HashRing()
        ring.add(index, new SimpleNode("NodeName", new TestServer("Server_name_1", 1)))
        
        when: "新しいIndexでノードが追加された場合、"
        def newIndex = 2
        ring.add(newIndex, new SimpleNode("NodeName", new TestServer("Server_name_2", 1)))
        
        then: "既に存在するNodeのSuccessorとなり、自分自身のSuccessorは存在するNodeとなる"
        ring.sorted_map.size() == 2
        ring.getSuccessor(index).index == newIndex
        ring.getSuccessor(newIndex).index == index
        
        then: "また既に存在するNodeのPredecessorは新しNodeで、新しいNodeのPredecessorは既に存在するNodeである"
        ring.getPredecessor(index).index == newIndex
        ring.getPredecessor(newIndex).index == index
    }
    
    def "add は 既にIndexの値が指定するIndexより高いNodeがHashRingに存在し、"(){
        given:
        def index = 2
        HashRing ring = new HashRing()
        ring.add(index, new SimpleNode("NodeName", new TestServer("Server_name_1", 1)))
        
        when: "新しいIndexでノードが追加された場合、"
        def newIndex = 1
        ring.add(newIndex, new SimpleNode("NodeName", new TestServer("Server_name_2", 1)))
        
        then: "既に存在するNodeのPredecessorとなり、自分自身のPredecessorは存在するNodeとなる"
        ring.sorted_map.size() == 2
        ring.getPredecessor(index).index == newIndex
        ring.getPredecessor(newIndex).index == index
        
        then: "また既に存在するNodeのSuccessorは新しNodeで、新しいNodeのSuccessorは既に存在するNodeである"
        ring.getSuccessor(index).index == newIndex
        ring.getSuccessor(newIndex).index == index
    }
    
    def "add は 既にIndexの値が指定するIndexより高いNodeがHashRingに2つ存在し、"(){
        given:
        def index2 = 2
        def index3 = 3
        HashRing ring = new HashRing()
        ring.add(index2, new SimpleNode("NodeName", new TestServer("Server_name_1", 1)))
        ring.add(index3, new SimpleNode("NodeName", new TestServer("Server_name_2", 1)))
        
        when: "新しいIndexでノードが追加された場合、"
        def newIndex = 1
        ring.add(newIndex, new SimpleNode("NodeName", new TestServer("Server_name_3", 1)))
        
        then: "既に存在する3番目のNodeのSuccessorとなり、自分自身のPredecessorは存在する3番目のNodeとなる"
        ring.sorted_map.size() == 3
        ring.getSuccessor(index3).index == newIndex
        ring.getPredecessor(newIndex).index == index3
        
        then: "また2番目に存在するNodeのPredecessorは新しNodeで、新しいNodeのSuccessorは2番目のNodeである"
        ring.getPredecessor(index2).index == newIndex
        ring.getSuccessor(newIndex).index == index2
    }
    
    def "add は 既にIndexの値が指定するIndexより低いNodeがHashRingに2つ存在し、"(){
        given:
        def index1 = 1
        def index2 = 2
        HashRing ring = new HashRing()
        ring.add(index1, new SimpleNode("NodeName", new TestServer("Server_name_1", 1)))
        ring.add(index2, new SimpleNode("NodeName", new TestServer("Server_name_2", 1)))
        
        when: "新しいIndexでノードが追加された場合、"
        def newIndex = 3
        ring.add(newIndex, new SimpleNode("NodeName", new TestServer("Server_name_3", 1)))
        
        then: "既に存在する2番目のNodeのSuccessorとなり、自分自身のPredecessorは存在する2番目のNodeとなる"
        ring.sorted_map.size() == 3
        ring.getSuccessor(index2).index == newIndex
        ring.getPredecessor(newIndex).index == index2
        
        then: "また1番目に存在するNodeのPredecessorは新しNodeで、新しいNodeのSuccessorは1番目のNodeである"
        ring.getPredecessor(index1).index == newIndex
        ring.getSuccessor(newIndex).index == index1
    }
    
    def "add は 指定するIndexが既に存在するIndexの間の場合、"(){
        given:
        def index1 = 1
        def index3 = 3
        HashRing ring = new HashRing()
        ring.add(index1, new SimpleNode("NodeName", new TestServer("Server_name_1", 1)))
        ring.add(index3, new SimpleNode("NodeName", new TestServer("Server_name_2", 1)))
        
        when: "新しいIndexでノードが追加された場合、"
        def newIndex = 2
        ring.add(newIndex, new SimpleNode("NodeName", new TestServer("Server_name_3", 1)))
        
        then: "既に存在する1番目のNodeのSuccessorとなり、自分自身のSuccessorは存在する3番目のNodeとなる"
        ring.sorted_map.size() == 3
        ring.getSuccessor(index1).index == newIndex
        ring.getSuccessor(newIndex).index == index3
        
        then: "また新しNodeのPredecessorは1番目のNodeで3番目のNodeのPredecessorは新しNodeである"
        ring.getPredecessor(newIndex).index == index1
        ring.getPredecessor(index3).index == newIndex
    }
    
    def "getPredecessor はソートされたマップが空の場合は null を返す"(){
        given:
        HashRing ring = new HashRing()
        
        when:
        def result = ring.getPredecessor(1)
        
        then:
        !result
    }
    
    def "getPredecessor は ソートされたマップから指定する index の直前のNodeを返す"(){
        given:
        HashRing ring = new HashRing()
        ring.sorted_map = [
            1L: new SimpleNode("NodeName", new TestServer("Server_name_1", 1)),
            2L: new SimpleNode("NodeName", new TestServer("Server_name_2", 1))
        ]
        
        when:
        def result = ring.getPredecessor(2L)
        
        then:
        result
        result == ring.sorted_map.get(1L)
    }
    
    def "getPredecessor は ソートされたマップから指定する index の直前の Node がない場合は最後のIndexのNodeを返す"(){
        given:
        HashRing ring = new HashRing()
        ring.sorted_map = [
            1L: new SimpleNode("NodeName", new TestServer("Server_name_1", 1)),
            2L: new SimpleNode("NodeName", new TestServer("Server_name_2", 1)),
            3L: new SimpleNode("NodeName", new TestServer("Server_name_3", 1))
        ]
        
        when:
        def result = ring.getPredecessor(1L)
        
        then:
        result
        result == ring.sorted_map.get(3L)
    }
    
    def "getSuccessor はソートされたマップが空の場合は null を返す"(){
        given:
        HashRing ring = new HashRing()
        
        when:
        def result = ring.getSuccessor(1)
        
        then:
        !result
    }
    
    def "getSuccessor は ソートされたマップから指定する index の直後のNodeを返す"(){
        given:
        HashRing ring = new HashRing()
        ring.sorted_map = [
            1L: new SimpleNode("NodeName", new TestServer("Server_name_1", 1)),
            2L: new SimpleNode("NodeName", new TestServer("Server_name_2", 1))
        ]
        
        when:
        def result = ring.getSuccessor(1L)
        
        then:
        result
        result == ring.sorted_map.get(2L)
    }
    
    def "getSuccessor は ソートされたマップから指定する index の直後の Node がない場合は最初のIndexのNodeを返す"(){
        given:
        HashRing ring = new HashRing()
        ring.sorted_map = [
            1L: new SimpleNode("NodeName", new TestServer("Server_name_1", 1)),
            2L: new SimpleNode("NodeName", new TestServer("Server_name_2", 1)),
            3L: new SimpleNode("NodeName", new TestServer("Server_name_3", 1))
        ]
        
        when:
        def result = ring.getSuccessor(3L)
        
        then:
        result
        result == ring.sorted_map.get(1L)
    }
	public class TestServer implements Client
	{
	
		String name;
		Integer id;
		int replicas = 0;
	
		int weight=1;
		public TestServer(){
			
		}
		/**
		 * @param name
		 * @param id
		 * @param host
		 * @throws UnknownHostException
		 */
		public TestServer(String name,int weight) throws UnknownHostException {
			super();
			this.name = name;
			this.id = new Random().nextInt();
			this.weight = weight;
		}
	
		public InetAddress getInetAddress() {
			// TODO Auto-generated method stub
			return null;
		}
	
		public InetSocketAddress getHost() {
			// TODO Auto-generated method stub
			return null;
		}
	
		public Integer getId() {
			// TODO Auto-generated method stub
			return this.id;
		}
	
		public String getName() {
			// TODO Auto-generated method stub
			return this.name;
		}
	
		public void setHost(String host,int port) throws UnknownHostException {
			// TODO Auto-generated method stub
		}
	
		public void setId(Integer id) {
			// TODO Auto-generated method stub
			this.id = id;
			
		}
	
		public void setName(String name) {
			// TODO Auto-generated method stub
			this.name = name;
			
		}
		
		public String toString(){
			return "["+TestServer.class+"]" +
					"name=" + this.name;
		}
	
		@Override
		public Object get(String key) {
			// TODO Auto-generated method stub
			return null;
		}
	
		@Override
		public boolean set(String key, Object value) {
			// TODO Auto-generated method stub
			return false;
		}
	
		@Override
		public int getWeight() {
			// TODO Auto-generated method stub
			return this.weight;
		}
	
		@Override
		public boolean isAvailable(boolean b) {
			// TODO Auto-generated method stub
			return true;
		}
	
		@Override
		public boolean setAvailable(boolean available) {
			// TODO Auto-generated method stub
			return true;
		}
		@Override
		public int getCloneNumber() {
			// TODO Auto-generated method stub
			return replicas;
		}
	
		@Override
		public void setCloneNumber(int clones) {
			// TODO Auto-generated method stub
			replicas = clones;
			
		}
	
		@Override
		public boolean delete(String key) throws Exception {
			// TODO Auto-generated method stub
			return false;
		}
	
		@Override
		public String getType() {
			// TODO Auto-generated method stub
			return null;
		}
	
		@Override
		public Set<String> keys() throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
	
		@Override
		public String getAdded() {
			// TODO Auto-generated method stub
			return null;
		}
	
		@Override
		public String getCreated() {
			// TODO Auto-generated method stub
			return null;
		}
	
		@Override
		public ClientException getFailure() {
			// TODO Auto-generated method stub
			return null;
		}
	
		@Override
		public void terminate() {
			// TODO Auto-generated method stub
			
		}
	
		public boolean delete(String[] keys) throws Exception {
			// TODO Auto-generated method stub
			return false;
		}
	
		public Map<String, Object> get(String[] keys) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
	
		public Set<String> keys(String pattern) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
	
		public boolean set(Map<String, Object> entries) throws Exception {
			// TODO Auto-generated method stub
			return false;
		}
	
		@Override
		public boolean clear() throws Exception {
			// TODO Auto-generated method stub
			return false;
		}
	
		@Override
		public int size() throws Exception {
			// TODO Auto-generated method stub
			return 0;
		}
	
		@Override
		public String getProperty(String key) {
			// TODO Auto-generated method stub
			return null;
		}
	
		@Override
		public void setClientEventListener(ClientEventListener listener) {
			// TODO Auto-generated method stub
			
		}
	}
}
