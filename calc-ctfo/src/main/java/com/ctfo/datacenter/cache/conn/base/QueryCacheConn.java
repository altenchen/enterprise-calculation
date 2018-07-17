package com.ctfo.datacenter.cache.conn.base;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract interface QueryCacheConn {
    public abstract Long queryTTL(String paramString)
        throws DataCenterException;

    public abstract String query(String paramString)
        throws DataCenterException;

    public abstract byte[] query(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract String queryListIndexValue(String paramString, Long paramLong)
        throws DataCenterException;

    public abstract List<String> queryList(String paramString, Long paramLong1, Long paramLong2)
        throws DataCenterException;

    public abstract List<byte[]> queryList(byte[] paramArrayOfByte, int paramInt1, int paramInt2)
        throws DataCenterException;

    public abstract String popRList(String paramString)
        throws DataCenterException;

    public abstract byte[] popRList(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract String popLList(String paramString)
        throws DataCenterException;

    public abstract byte[] popLList(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract Long queryListSize(String paramString)
        throws DataCenterException;

    public abstract Set<String> querySet(String paramString)
        throws DataCenterException;

    public abstract Set<byte[]> querySet(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract Long querySetSize(String paramString)
        throws DataCenterException;

    public abstract Set<String> querySortedSet(String paramString, long paramLong1, long paramLong2)
        throws DataCenterException;

    public abstract Set<String> querySortedSetByScore(String paramString, double paramDouble1, double paramDouble2)
        throws DataCenterException;

    public abstract Set<byte[]> querySortedSet(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract Long querySortedSetSize(String paramString)
        throws DataCenterException;

    public abstract Map<String, String> queryHash(String paramString)
        throws DataCenterException;

    public abstract Map<byte[], byte[]> queryHash(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract Set<String> queryHashField(String paramString)
        throws DataCenterException;

    public abstract Set<byte[]> queryHashField(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract String queryHash(String paramString1, String paramString2)
        throws DataCenterException;

    public abstract byte[] queryHash(byte[] paramArrayOfByte1, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract List<String> queryHash(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract List<byte[]> queryHash(byte[] paramArrayOfByte, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long queryHashSize(String paramString)
        throws DataCenterException;

    public abstract Boolean isSetValueExist(String paramString1, String paramString2)
        throws DataCenterException;

    public abstract Boolean isKeyExist(String paramString)
        throws DataCenterException;
}
