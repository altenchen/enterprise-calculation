package com.ctfo.datacenter.cache.message;

import com.ctfo.datacenter.cache.conn.ConfigControl;
import com.ctfo.datacenter.cache.conn.impl.ConfigControlImpl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.util.RedisUtil;
import redis.clients.jedis.Jedis;

public class MessageThread
    extends Thread {
    private ConfigControl configControl;
    private String[] channels;
    private String shardStatus = "0";
    private String address;

    public MessageThread(String address, String... channels) {
        this.address = address;
        this.channels = channels;
    }

    public String getShardStatus()
        throws DataCenterException {
        String shardstatuskey = RedisUtil.getConfigKey("shardchannelstatus");
        this.configControl = new ConfigControlImpl(this.address);
        this.shardStatus = this.configControl.query(shardstatuskey);
        if (this.shardStatus != null) {
            return this.shardStatus;
        }
        return "0";
    }

    @Override
    public void run() {
        Jedis jedis = null;
        try {
            PubSubListener pubSubListener = new PubSubListener(this.address);
            jedis = this.configControl.getConnection();
            jedis.subscribe(pubSubListener, this.channels);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.configControl.freeConnection(jedis);
        }
    }
}
