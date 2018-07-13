package com.ctfo.datacenter.cache.config;

import com.ctfo.datacenter.cache.conn.ConfigControl;
import com.ctfo.datacenter.cache.conn.impl.ConfigControlImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class CTFOAddCacheDeleteThread
    extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(CTFOAddCacheDeleteThread.class);
    private String address;
    private String listkey;
    private CountDownLatch latch;
    private String configAddress;
    private int threadNum;

    public CTFOAddCacheDeleteThread(int threadNum, String configAddress, String key, String address, CountDownLatch latch) {
        this.address = address;
        this.listkey = key;
        this.latch = latch;
        this.configAddress = configAddress;
        this.threadNum = threadNum;
    }

    @Override
    public void run() {
        ConfigControl c = null;
        ConfigControl configControl = null;
        try {
            c = new ConfigControlImpl(this.configAddress);
            configControl = new ConfigControlImpl(this.address);

            int i = 0;
            for (; ; ) {
                String key = c.popLList(this.listkey);
                if (key == null) {
                    break;
                }
                configControl.delete(key);

                i++;
            }
            StringBuffer sb = new StringBuffer();
            sb.append("add cache[delete key,thread:");
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
            c.destoryConnection();
            this.latch.countDown();
        }
    }
}
