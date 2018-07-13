package com.ctfo.datacenter.cache.handle.base;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract interface QueryCacheTable {
    public abstract Long queryTTL(String paramString)
        throws DataCenterException;

    public abstract String query(String paramString)
        throws DataCenterException;

    public abstract byte[] queryByte(String paramString)
        throws DataCenterException;

    public abstract String queryListIndexValue(String paramString, Long paramLong)
        throws DataCenterException;

    public abstract List<String> queryList(String paramString)
        throws DataCenterException;

    public abstract List<byte[]> queryListByte(String paramString)
        throws DataCenterException;

    public abstract String popRList(String paramString)
        throws DataCenterException;

    public abstract byte[] popRListByte(String paramString)
        throws DataCenterException;

    public abstract String popLList(String paramString)
        throws DataCenterException;

    public abstract byte[] popLListByte(String paramString)
        throws DataCenterException;

    public abstract Long queryListSize(String paramString)
        throws DataCenterException;

    public abstract Set<String> querySet(String paramString)
        throws DataCenterException;

    public abstract Set<byte[]> querySetByte(String paramString)
        throws DataCenterException;

    public abstract Long querySetSize(String paramString)
        throws DataCenterException;

    public abstract Set<String> querySortedSet(String paramString)
        throws DataCenterException;

    public abstract Set<String> querySortedSet(String paramString, long paramLong1, long paramLong2)
        throws DataCenterException;

    public abstract Set<String> querySortedSetByScore(String paramString, double paramDouble1, double paramDouble2)
        throws DataCenterException;

    public abstract Set<byte[]> querySortedSetByte(String paramString)
        throws DataCenterException;

    public abstract Long querySortedSetSize(String paramString)
        throws DataCenterException;

    public abstract Map<String, String> queryHash(String paramString)
        throws DataCenterException;

    public abstract Map<byte[], byte[]> queryHashByte(String paramString)
        throws DataCenterException;

    public abstract Set<String> queryHashField(String paramString)
        throws DataCenterException;

    public abstract Set<byte[]> queryHashFieldByte(String paramString)
        throws DataCenterException;

    public abstract String queryHash(String paramString1, String paramString2)
        throws DataCenterException;

    public abstract byte[] queryHashByte(String paramString, byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract List<String> queryHash(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract List<byte[]> queryHash(String paramString, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long queryHashSize(String paramString)
        throws DataCenterException;

    public abstract Boolean isSetValueExist(String paramString1, String paramString2)
        throws DataCenterException;

    public abstract Boolean isKeyExist(String paramString)
        throws DataCenterException;

    public abstract String queryLList(String paramString)
        throws DataCenterException;

    public abstract byte[] queryLListByte(String paramString)
        throws DataCenterException;

    public abstract String queryRList(String paramString)
        throws DataCenterException;

    public abstract byte[] queryRListByte(String paramString)
        throws DataCenterException;
}
