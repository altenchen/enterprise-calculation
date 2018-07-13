package com.ctfo.datacenter.cache.message;

public class ShardPubSubDeal {
    private String address;

    protected ShardPubSubDeal() {
    }

    protected ShardPubSubDeal(String address) {
        this.address = address;
    }

    public void subPubStartDeal() {
        PubSubManager.setShardStatus(this.address, "1");
    }

    public void subPubEndDeal() {
        PubSubManager.setShardStatus(this.address, "6");
    }

    public void subPubReplaceDeal() {
        PubSubManager.setShardStatus(this.address, "R");
    }
}
