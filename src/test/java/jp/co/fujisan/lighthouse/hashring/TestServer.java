package jp.co.fujisan.lighthouse.hashring;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import jp.co.fujisan.lighthouse.client.Client;
import jp.co.fujisan.lighthouse.client.ClientEventListener;
import jp.co.fujisan.lighthouse.client.ClientException;

public class TestServer implements Client {

    String name;
    Integer id;
    int replicas = 0;

    int weight = 1;

    public TestServer() {
    }

    /**
     * @param name
     * @param id
     * @param host
     * @throws UnknownHostException
     */
    public TestServer(String name, int weight) throws UnknownHostException {
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

    public void setHost(String host, int port) throws UnknownHostException {
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

    public String toString() {
        return "[" + TestServer.class + "]" + "name=" + this.name;
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
    public String getRingId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setClientEventListener(ClientEventListener listener) {
        // TODO Auto-generated method stub

    }

}
