package com.ctfo.datacenter.cache.config;

import com.ctfo.datacenter.cache.conn.ConfigControl;
import com.ctfo.datacenter.cache.conn.ShardControl;
import com.ctfo.datacenter.cache.conn.impl.ConfigControlImpl;
import com.ctfo.datacenter.cache.conn.impl.ShardControlImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class CTFOAddCacheMoveThread
    extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(CTFOAddCacheMoveThread.class);
    private String address;
    private String listkey;
    private CountDownLatch latch;
    private String configAddress;
    private int threadNum;
    private Map<String, String> map;

    public CTFOAddCacheMoveThread(int threadNum, String configAddress, String key, String address, Map<String, String> map, CountDownLatch latch) {
        this.address = address;
        this.map = map;
        this.listkey = key;
        this.latch = latch;
        this.configAddress = configAddress;
        this.threadNum = threadNum;
    }

    @Override
    public void run() {
        ConfigControl c = null;
        ConfigControl configControl = null;
        ShardControl addShardedControl = null;
        try {
            c = new ConfigControlImpl(this.configAddress);
            configControl = new ConfigControlImpl(this.address);
            addShardedControl = new ShardControlImpl(this.map);
            int i = 0;
            for (; ; ) {
                String key = c.popLList(this.listkey);
                if (key == null) {
                    break;
                }
                String type = configControl.queryType(key);
                int seconds = configControl.queryTTL(key).intValue();
                if ("string".equals(type)) {
                    String value = configControl.query(key);
                    addShardedControl.add(key, seconds, value);
                } else if ("list".equals(type)) {
                    List<String> list = configControl.queryList(key, Long.valueOf(0L), Long.valueOf(-1L));
                    if (list.size() > 0) {
                        String[] values = (String[]) list.toArray(new String[list.size()]);
                        addShardedControl.addRList(key, seconds, values);
                    }
                } else if ("hash".equals(type)) {
                    Map<String, String> values = configControl.queryHash(key);
                    addShardedControl.addHash(key, seconds, values);
                } else if ("set".equals(type)) {
                    Set<String> setkey = configControl.querySet(key);
                    String[] values = (String[]) setkey.toArray(new String[setkey.size()]);
                    addShardedControl.addSet(key, seconds, values);
                } else if ("zset".equals(type)) {
                    Set<Tuple> zsetkey = configControl.queryObjectSortedSet(key);
                    for (Tuple tuple : zsetkey) {
                        addShardedControl.addSortedSet(key, seconds, tuple.getScore(), tuple.getElement());
                    }
                } else {
                    if ("none".equals(type)) {
                        continue;
                    }
                }
                i++;
            }
            StringBuffer sb = new StringBuffer();
            sb.append("add cache[move key,thread:");
            sb.append(this.threadNum);
            sb.append(",");
            sb.append(this.address);
            sb.append(", key size:");
            sb.append(i);
            sb.append("]");
            logger.info(sb.toString());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        } finally {
            configControl.destoryConnection();
            addShardedControl.destoryPool();
            c.destoryConnection();
            this.latch.countDown();
        }
    }
}
