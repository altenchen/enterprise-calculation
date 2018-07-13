package com.ctfo.datacenter.cache.message;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PubSubManager {
    private static Map<String, String> map = new ConcurrentHashMap();

    private static synchronized String init(String address, String... channels)
        throws DataCenterException {
        MessageThread messageThread = new MessageThread(address, channels);
        String shardStatus = messageThread.getShardStatus();
        messageThread.start();
        map.put(address, shardStatus);
        return shardStatus;
    }

    public static String initShardStatus(String address, String... channels)
        throws DataCenterException {
        String shardstatus = (String) map.get(address);
        if (shardstatus == null) {
            return init(address, channels);
        }
        return shardstatus;
    }

    public static void setShardStatus(String address, String shardStatus) {
        map.put(address, shardStatus);
    }

    public static String getShardStatus(String address) {
        return (String) map.get(address);
    }
}
