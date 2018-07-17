package com.ctfo.datacenter.cache.message;

import redis.clients.jedis.JedisPubSub;

public class PubSubListener
    extends JedisPubSub {
    private String address;

    protected PubSubListener() {
    }

    protected PubSubListener(String address) {
        this.address = address;
    }

    @Override
    public void onMessage(String channel, String message) {
        if ("shardchannel".equals(channel)) {
            ShardPubSubDeal shardPubSubDeal = new ShardPubSubDeal(this.address);
            if ("1".equals(message)) {
                shardPubSubDeal.subPubStartDeal();
            } else if ("0".equals(message)) {
                shardPubSubDeal.subPubEndDeal();
            } else if ("R".equals(message)) {
                shardPubSubDeal.subPubReplaceDeal();
            }
        }
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {
    }
}
