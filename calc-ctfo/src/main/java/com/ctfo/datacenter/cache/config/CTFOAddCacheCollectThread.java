package com.ctfo.datacenter.cache.config;

import com.ctfo.datacenter.cache.conn.ConfigControl;
import com.ctfo.datacenter.cache.conn.ShardControl;
import com.ctfo.datacenter.cache.conn.impl.ConfigControlImpl;
import com.ctfo.datacenter.cache.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class CTFOAddCacheCollectThread
    extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(CTFOAddCacheCollectThread.class);
    private CountDownLatch latch;
    private String configAddress;
    private int threadNum;
    private Jedis jedis;
    private ShardControl shardControl;
    private ShardControl addShardControl;
    private Thread.UncaughtExceptionHandler handler;

    public CTFOAddCacheCollectThread(int threadNum, String configAddress, Jedis jedis, ShardControl shardControl, ShardControl addShardControl, CountDownLatch latch) {
        this.latch = latch;
        this.threadNum = threadNum;
        this.configAddress = configAddress;
        this.jedis = jedis;
        this.shardControl = shardControl;
        this.addShardControl = addShardControl;
    }

    @Override
    public void run() {
        ConfigControl c = null;
        try {
            c = new ConfigControlImpl(this.configAddress);
            String jedishost = this.jedis.getClient().getHost() + ":" + this.jedis.getClient().getPort();
            String upgradekey = RedisUtil.getConfigKey("upgrade", jedishost);
            String deletekey = RedisUtil.getConfigKey("delete", jedishost);

            ScanParams sp = new ScanParams();
            sp.count(10000);
            sp.match("*");

            String cursor = "0";

            Set<String> tempSet = new HashSet();
            int count = 0;
            for (; ; ) {
                ScanResult<String> sr = this.jedis.scan(cursor, sp);
                cursor = sr.getStringCursor();
                List<String> list = sr.getResult();
                if ((cursor == null) || ("0".equals(cursor))) {
                    for (int i = 0; i < list.size(); i++) {
                        String host = this.shardControl.queryAddress((String) list.get(i));
                        String addhost = this.addShardControl.queryAddress((String) list.get(i));
                        if (!host.equals(addhost)) {
                            tempSet.add(list.get(i));
                            count++;
                        }
                    }
                    String[] values = (String[]) tempSet.toArray(new String[tempSet.size()]);
                    c.addRList(upgradekey, values);
                    c.addRList(deletekey, values);
                    tempSet = new HashSet();
                    break;
                }
                for (int i = 0; i < list.size(); i++) {
                    String host = this.shardControl.queryAddress((String) list.get(i));
                    String addhost = this.addShardControl.queryAddress((String) list.get(i));
                    if (!host.equals(addhost)) {
                        tempSet.add(list.get(i));
                        count++;
                    }
                }
            }
            StringBuffer sb = new StringBuffer();
            sb.append("add cache[collect key,thread:");
            sb.append(this.threadNum);
            sb.append(",");
            sb.append(jedishost);
            sb.append(",key size:");
            sb.append(count);
            sb.append("]");
            logger.info(sb.toString());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        } finally {
            c.destoryConnection();

            this.latch.countDown();
        }
    }

    public Thread.UncaughtExceptionHandler getHandler() {
        return this.handler;
    }

    public void setHandler(Thread.UncaughtExceptionHandler handler) {
        this.handler = handler;
    }
}
