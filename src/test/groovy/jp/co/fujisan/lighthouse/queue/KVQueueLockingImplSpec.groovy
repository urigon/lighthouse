package jp.co.fujisan.lighthouse.queue

import spock.lang.*


import static org.junit.Assert.*
import jp.co.fujisan.lighthouse.queue.exception.AlreadyFinalizedException 
import jp.co.fujisan.lighthouse.queue.exception.EnqueueTimeoutException 
import jp.co.fujisan.lighthouse.queue.exception.LockTimeoutException 
import org.gmock.WithGMock
import org.junit.runner.RunWith

import org.spockframework.runtime.Sputnik
import org.springframework.util.StopWatch 
import spock.lang.*

@WithGMock
@RunWith(Sputnik)
class KVQueueLockingImplSpec extends Specification {
    def "enqueue は 有効でなければAlreadyFinalizedExceptionを投げる"(){
        setup:
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        queue.isAvailable = false
        
        when:
        def result = queue.enqueue(null)
        
        then:
        thrown(AlreadyFinalizedException)
    }
    
    def "enqueue は指定されたitemが null だと null を返す"(){
        setup:
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        
        when:
        def result = queue.enqueue(null)
        
        then:
        !result
    }
    
    def "enqueue は指定された item の command が CANCEL の場合、"(){
        setup:
        def key = "key1"
        
        when: "removeを呼び出し"
        def removeCalled = false
        def out_item_mock = mock(QueueItem)
        out_item_mock.unlock()
        
        def item = new QueueItem(QueueItem.CMD_CANCEL, key)
        def in_item_mock = mock(item)
        in_item_mock.unlock()
        /*
         * KVQueueLockingImpl queue = new KVQueueLockingImpl()
         * queue.metaClass.remove = {String key2 -> ... }
         * 上記の方法ではうまくmetaClassで定義したメソッドが実行されないので以下の方法で
         * Overrideする
         */
        KVQueueLockingImpl queue = new KVQueueLockingImpl() {
            public QueueItem remove(String key2) throws AlreadyFinalizedException{
                if(key2 == key){
                    removeCalled = true
                }
                return out_item_mock
            }
        }
        
        def result = null
        play{
            result = queue.enqueue(in_item_mock)
        }
        
        then: "inputとoutputのitemのロックを解除し戻り値を返還する"
        removeCalled
        result == out_item_mock
    }
    
    def "enqueue は指定された item の command が GET の場合、"(){
        setup:
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        def key = "key1"
        
        when: "該当するキャッシュの情報を取得し"
        def out_item_mock = mock(QueueItem)
        out_item_mock.unlock()
        
        def item = new QueueItem(QueueItem.CMD_GET, key)
        def in_item_mock = mock(item)
        in_item_mock.unlock()
        
        def m_cache_mock = mock(LockingConcurrentHashMap)
        m_cache_mock.get(key).returns(out_item_mock)
        
        queue.m_cache = m_cache_mock
        def result = null
        play{
            result = queue.enqueue(in_item_mock)
        }
        
        then: "inputとoutputのitemのロックを解除し戻り値を返還する"
        result == out_item_mock
    }
    
    def "enqueue は指定された item の command が SET の場合、"(){
        setup:
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        def key = "key1"
        
        when: "与えられた情報をキャッシュに書き込み"
        def out_item_mock = mock(QueueItem)
        out_item_mock.unlock()
        
        def item = new QueueItem(QueueItem.CMD_SET, key)
        def in_item_mock = mock(item)
        in_item_mock.unlock()
        
        def m_cache_mock = mock(LockingConcurrentHashMap)
        m_cache_mock.put(key, in_item_mock, LockingConcurrentHashMap.LOCK_WRITE).returns(out_item_mock)
        queue.m_cache = m_cache_mock
        
        and: "キューに指定するKeyが存在しない場合はキューに書き込み"
        def m_queue_mock = mock(RamdomAccessRemovalConcurrentLinkedQueue)
        m_queue_mock.contains(key).returns(false)
        m_queue_mock.offer(key).returns(true)
        queue.m_queue = m_queue_mock
        
        def result = null
        play{
            result = queue.enqueue(in_item_mock)
        }
        
        then: "inputとoutputのitemのロックを解除し戻り値を返還する"
        result == out_item_mock
    }
    
    def "enqueue は指定された item の command が DELETE の場合、"(){
        setup:
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        def key = "key1"
        
        when: "与えられた情報をキャッシュとキューに書き込み"
        def out_item_mock = mock(QueueItem)
        out_item_mock.unlock()
        
        def item = new QueueItem(QueueItem.CMD_DELETE, key)
        def in_item_mock = mock(item)
        in_item_mock.unlock()
        
        def m_cache_mock = mock(LockingConcurrentHashMap)
        m_cache_mock.put(key, in_item_mock, LockingConcurrentHashMap.LOCK_WRITE).returns(out_item_mock)
        queue.m_cache = m_cache_mock
        
        and: "キューに指定するKeyが存在する場合はキューに書きこまず"
        def m_queue_mock = mock(RamdomAccessRemovalConcurrentLinkedQueue)
        m_queue_mock.contains(key).returns(true)
        m_queue_mock.offer(key).never()
        queue.m_queue = m_queue_mock
        
        def result = null
        play{
            result = queue.enqueue(in_item_mock)
        }
        
        then: "inputとoutputのitemのロックを解除し戻り値を返還する"
        result == out_item_mock
    }
    
    def "dequeue は 有効でなければAlreadyFinalizedExceptionを投げる"(){
        setup:
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        queue.isAvailable = false
        
        when:
        def result = queue.dequeue()
        
        then:
        thrown(AlreadyFinalizedException)
        
    }
    
    def "dequeue は キューから poll した時に key が存在する場合"(){
        setup:
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        def key = "key1"
        
        when:
        def m_queue_mock = mock(RamdomAccessRemovalConcurrentLinkedQueue)
        m_queue_mock.poll().returns(key)
        queue.m_queue = m_queue_mock
        
        and: "キャッシュからitemを取得しCommiterに自分自身をセットして"
        def out_item_mock = mock(QueueItem)
        out_item_mock.commiter.set(queue)
        
        def m_cache_mock = mock(LockingConcurrentHashMap)
        m_cache_mock.get(key, LockingConcurrentHashMap.LOCK_WRITE).returns(out_item_mock)
        queue.m_cache = m_cache_mock
        
        def result = null
        play{
            result = queue.dequeue()
        }
        
        then: "そのアイテムを返す"
        result == out_item_mock
    }
    
    def "dequeue は キューから poll した時に key が存在しない場合"(){
        setup:
        def wait_for_enqueue = 2000
        def stopWatch = new StopWatch()
        stopWatch.start()
        
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        def key = "key1"
        def item = new QueueItem(QueueItem.CMD_SET, key)
        
        when: "別スレッドからその key が enqueue されるまで待つ"
        Thread.start{
            sleep(wait_for_enqueue)
            queue.enqueue(item)
        }
        def result = queue.dequeue()
        stopWatch.stop()
        
        then: "そのアイテムを返す"
        stopWatch.lastTaskTimeMillis > wait_for_enqueue
        result == item
    }
    
    def "remove は 有効でなければAlreadyFinalizedExceptionを投げる"(){
        setup:
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        queue.isAvailable = false
        
        when:
        def result = queue.remove(null)
        
        then:
        thrown(AlreadyFinalizedException)
    }
    
    def "remove は指定する key が null の場合は NullPointerExceptionを投げる"(){
        setup:
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        
        when:
        def result = queue.remove(null)
        
        then:
        thrown(NullPointerException)
    }
    
    def "remove は指定する key のエントリがキャッシュに存在した場合"(){
        setup:
        def key = "key1"
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        
        when: "キャッシュとキューのアイテムを削除しアイテムをアンロックし"
        def item_mock = mock(QueueItem)
        item_mock.unlock()
        
        def m_cache_mock = mock(LockingConcurrentHashMap)
        m_cache_mock.get(key, LockingConcurrentHashMap.LOCK_WRITE).returns(item_mock)
        m_cache_mock.remove(key)
        
        def m_queue_mock = mock(RamdomAccessRemovalConcurrentLinkedQueue)
        m_queue_mock.remove(key)
        
        queue.m_cache = m_cache_mock
        queue.m_queue = m_queue_mock
        
        def result = null
        play{
            result = queue.remove(key)
        }
        
        then:"キューから削除されたitemを返還する"
        result == item_mock
    }
    
    def "remove は指定する key のエントリがキャッシュに存在しない場合"(){
        setup:
        def key = "key1"
        QueueItem item = new QueueItem(key, null)
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        
        def m_cache_mock = mock(LockingConcurrentHashMap)
        m_cache_mock.get(key, LockingConcurrentHashMap.LOCK_WRITE).returns(null)
        
        when: "キューのアイテムを削除し"
        def m_queue_mock = mock(RamdomAccessRemovalConcurrentLinkedQueue)
        m_queue_mock.remove(key)
        
        queue.m_cache = m_cache_mock
        queue.m_queue = m_queue_mock
        
        def result = null
        play{
            result = queue.remove(key)
        }
        
        then:"nullを返す"
        !result
    }
    
    def "remove は 指定するkeyのエントリを取得中に LockTimeoutException が発生すると"(){
        setup:
        def key = "key1"
        QueueItem item = new QueueItem(key, null)
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        
        def m_cache_mock = mock(LockingConcurrentHashMap)
        m_cache_mock.get(key, LockingConcurrentHashMap.LOCK_WRITE).raises(new LockTimeoutException())
        
        when:
        queue.m_cache = m_cache_mock
        
        def result = null
        play{
            result = queue.remove(key)
        }
        
        then:"nullを返す"
        !result
    }
    def "put は 有効でなければAlreadyFinalizedExceptionを投げる"(){
        setup:
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        queue.isAvailable = false
        
        when:
        def result = queue.put(null, null)
        
        then:
        thrown(AlreadyFinalizedException)
    }
    
    def "put は key または item が null であれば null を返す"(){
        setup:
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        
        when:
        def result = queue.put(null, null)
        
        then:
        !result
        
        when:
        result = queue.put("key", null)
        
        then:
        !result
        
        when:
        result = queue.put(null, new QueueItem(null, null))
        
        then:
        !result
    }
    
    
    def "put は指定する key を元に item をキャッシュに格納する"(){
        setup:
        def key = "key1"
        def item = new QueueItem(key, null)
        
        def item_mock = mock(QueueItem)
        item_mock.unlock()
        
        def m_cache_mock = mock(LockingConcurrentHashMap)
        m_cache_mock.put(key, item, LockingConcurrentHashMap.LOCK_READ).returns(item_mock)
        
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        queue.m_cache = m_cache_mock
        
        when:
        def result = null
        play{
            result = queue.put(key, item)
        }
        
        then:
        result == item_mock
    }
    
    def "put は指定する key を元に item をキャッシュに格納する際に LockTimeoutException が発生すると nullを返す"(){
        setup:
        def key = "key1"
        def item = new QueueItem(key, null)
        
        def m_cache_mock = mock(LockingConcurrentHashMap)
        m_cache_mock.put(key, item, LockingConcurrentHashMap.LOCK_READ).raises(new LockTimeoutException())
        
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        queue.m_cache = m_cache_mock
        
        when:
        def result = null
        play{
            result = queue.put(key, item)
        }
        
        then:
        !result
    }
    
    def "put は指定する key を元に item をキャッシュに格納する際に InterruptedException が発生すると nullを返す"(){
        setup:
        def key = "key1"
        def item = new QueueItem(key, null)
        
        def m_cache_mock = mock(LockingConcurrentHashMap)
        m_cache_mock.put(key, item, LockingConcurrentHashMap.LOCK_READ).raises(new InterruptedException())
        
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        queue.m_cache = m_cache_mock
        
        when:
        def result = null
        play{
            result = queue.put(key, item)
        }
        
        then:
        !result
    }
    
    def "waitOnCheckQueueLimit は "(){
        setup:
        KVQueueLockingImpl queue = new KVQueueLockingImpl()
        
        when: "有効であり"
        queue.isAvailable = true
        
        and: "m_queue_limit が 0 以上であり"
        queue.m_queue_limit = 1
        
        and: "m_queue が 存在し"
        def m_queue = new RamdomAccessRemovalConcurrentLinkedQueue()
        
        and: "m_queue_limit が m_queue のサイズより低く"
        queue.m_queue.offer("key1")
        queue.m_queue.offer("key2")
        
        and: "m_cache が 存在し"
        queue.m_cache = new LockingConcurrentHashMap()
        
        and: "m_queue_limit が m_cache のサイズより低く"
        queue.m_cache.put("key1", new QueueItem("key1",null))
        queue.m_cache.put("key2", new QueueItem("key2",null))
        
        and: "m_enqueue_wait_ms が 1 の場合"
        queue.m_enqueue_wait_ms = 1
        
        queue.waitOnCheckQueueLimit()
        
        then: "EnqueueTimeoutException が投げられる"
        thrown(EnqueueTimeoutException)
        
        when: "m_enqueue_wait_ws が 10000で"
        queue.m_enqueue_wait_ms = 10000
        and: "キューのサイズがリミット以上だと無限ループに入るが"
        def wait_end = false
        Thread.start{
            queue.waitOnCheckQueueLimit()
            wait_end = true
        }
        queue.dequeue()
        queue.dequeue()
        sleep(3000)
        
        then: ""
        notThrown(EnqueueTimeoutException)
        wait_end
    }
}

