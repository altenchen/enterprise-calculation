package com.ctfo.datacenter.cache.conn;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.List;
import java.util.Map;

public abstract interface ShardPipelineControl {
    public abstract void close()
        throws DataCenterException;

    public abstract void sync()
        throws DataCenterException;

    public abstract void add(String paramString1, int paramInt, String paramString2)
        throws DataCenterException;

    public abstract void add(byte[] paramArrayOfByte1, int paramInt, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract void addRList(String paramString, int paramInt, String... paramVarArgs)
        throws DataCenterException;

    public abstract void addRList(byte[] paramArrayOfByte, int paramInt, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void addLList(String paramString, int paramInt, String... paramVarArgs)
        throws DataCenterException;

    public abstract void addLList(byte[] paramArrayOfByte, int paramInt, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void addSet(String paramString, int paramInt, String... paramVarArgs)
        throws DataCenterException;

    public abstract void addSet(byte[] paramArrayOfByte, int paramInt, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void addSortedSet(String paramString1, int paramInt, double paramDouble, String paramString2)
        throws DataCenterException;

    public abstract void addSortedSet(byte[] paramArrayOfByte1, int paramInt, double paramDouble, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract void addHash(String paramString1, int paramInt, String paramString2, String paramString3)
        throws DataCenterException;

    public abstract void addHash(byte[] paramArrayOfByte1, int paramInt, byte[] paramArrayOfByte2, byte[] paramArrayOfByte3)
        throws DataCenterException;

    public abstract void addHash(String paramString, int paramInt, Map<String, String> paramMap)
        throws DataCenterException;

    public abstract void addHash(byte[] paramArrayOfByte, int paramInt, Map<byte[], byte[]> paramMap)
        throws DataCenterException;

    public abstract void expire(String paramString, int paramInt)
        throws DataCenterException;

    public abstract void queryObjectSortedSet(String paramString)
        throws DataCenterException;

    public abstract void queryType(String paramString)
        throws DataCenterException;

    public abstract void queryTTL(String paramString)
        throws DataCenterException;

    public abstract void query(String paramString)
        throws DataCenterException;

    public abstract void query(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract void queryList(String paramString, Long paramLong1, Long paramLong2)
        throws DataCenterException;

    public abstract void queryList(byte[] paramArrayOfByte, int paramInt1, int paramInt2)
        throws DataCenterException;

    public abstract void popRList(String paramString)
        throws DataCenterException;

    public abstract void popRList(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract void popLList(String paramString)
        throws DataCenterException;

    public abstract void popLList(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract void queryListSize(String paramString)
        throws DataCenterException;

    public abstract void querySet(String paramString)
        throws DataCenterException;

    public abstract void querySet(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract void querySetSize(String paramString)
        throws DataCenterException;

    public abstract void querySortedSet(String paramString, long paramLong1, long paramLong2)
        throws DataCenterException;

    public abstract void querySortedSetByScore(String paramString, double paramDouble1, double paramDouble2)
        throws DataCenterException;

    public abstract void querySortedSet(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract void querySortedSetSize(String paramString)
        throws DataCenterException;

    public abstract void queryHash(String paramString)
        throws DataCenterException;

    public abstract void queryHash(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract void queryHashField(String paramString)
        throws DataCenterException;

    public abstract void queryHashField(byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract void queryHash(String paramString1, String paramString2)
        throws DataCenterException;

    public abstract void queryHash(byte[] paramArrayOfByte1, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract void queryHash(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract void queryHash(byte[] paramArrayOfByte, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void queryHashSize(String paramString)
        throws DataCenterException;

    public abstract void isSetValueExist(String paramString1, String paramString2)
        throws DataCenterException;

    public abstract void isKeyExist(String paramString)
        throws DataCenterException;

    public abstract void delete(String paramString)
        throws DataCenterException;

    public abstract void deleteHashField(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract void deleteHashField(byte[] paramArrayOfByte, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void deleteSetValue(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract void deleteSetValue(byte[] paramArrayOfByte, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void deleteSortedSetByRank(String paramString, long paramLong1, long paramLong2)
        throws DataCenterException;

    public abstract void deleteSortedSetByScore(String paramString, double paramDouble1, double paramDouble2)
        throws DataCenterException;

    public abstract void deleteSortedSetValue(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract void deleteSortedSetValue(byte[] paramArrayOfByte, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void deleteListValue(String paramString1, long paramLong, String paramString2)
        throws DataCenterException;

    public abstract void deleteListValue(byte[] paramArrayOfByte1, int paramInt, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract void incr(String paramString)
        throws DataCenterException;

    public abstract void incr(String paramString, int paramInt)
        throws DataCenterException;

    public abstract void decr(String paramString)
        throws DataCenterException;

    public abstract void decr(String paramString, int paramInt)
        throws DataCenterException;

    public abstract List<Object> syncAndReturnAll()
        throws DataCenterException;

    public abstract void disconnect()
        throws DataCenterException;

    public abstract void reconnectPipeline()
        throws DataCenterException;
}
