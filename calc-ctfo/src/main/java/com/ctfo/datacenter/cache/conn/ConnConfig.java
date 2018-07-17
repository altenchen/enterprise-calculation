package com.ctfo.datacenter.cache.conn;

public class ConnConfig {
    private int SHARDPOOL_MAXACTIVE = 10000;
    private int SHARDPOOL_MAXIDLE = 1000;
    private int SHARDPOOL_MAXWAIT = 10000;
    private int SHARDPOOL_MINIDLE = 0;
    private boolean SHARDPOOL_TESTONBORROW = false;
    private int SHARDPOOL_TIMEOUT = 10000;

    public int getSHARDPOOL_MAXACTIVE() {
        return this.SHARDPOOL_MAXACTIVE;
    }

    public void setSHARDPOOL_MAXACTIVE(int sHARDPOOL_MAXACTIVE) {
        this.SHARDPOOL_MAXACTIVE = sHARDPOOL_MAXACTIVE;
    }

    public int getSHARDPOOL_MAXIDLE() {
        return this.SHARDPOOL_MAXIDLE;
    }

    public void setSHARDPOOL_MAXIDLE(int sHARDPOOL_MAXIDLE) {
        this.SHARDPOOL_MAXIDLE = sHARDPOOL_MAXIDLE;
    }

    public int getSHARDPOOL_MAXWAIT() {
        return this.SHARDPOOL_MAXWAIT;
    }

    public void setSHARDPOOL_MAXWAIT(int sHARDPOOL_MAXWAIT) {
        this.SHARDPOOL_MAXWAIT = sHARDPOOL_MAXWAIT;
    }

    public int getSHARDPOOL_MINIDLE() {
        return this.SHARDPOOL_MINIDLE;
    }

    public void setSHARDPOOL_MINIDLE(int sHARDPOOL_MINIDLE) {
        this.SHARDPOOL_MINIDLE = sHARDPOOL_MINIDLE;
    }

    public boolean isSHARDPOOL_TESTONBORROW() {
        return this.SHARDPOOL_TESTONBORROW;
    }

    public void setSHARDPOOL_TESTONBORROW(boolean sHARDPOOL_TESTONBORROW) {
        this.SHARDPOOL_TESTONBORROW = sHARDPOOL_TESTONBORROW;
    }

    public int getSHARDPOOL_TIMEOUT() {
        return this.SHARDPOOL_TIMEOUT;
    }

    public void setSHARDPOOL_TIMEOUT(int sHARDPOOL_TIMEOUT) {
        this.SHARDPOOL_TIMEOUT = sHARDPOOL_TIMEOUT;
    }
}
