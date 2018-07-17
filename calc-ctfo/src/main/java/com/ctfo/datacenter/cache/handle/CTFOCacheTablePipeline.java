package com.ctfo.datacenter.cache.handle;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.List;
import java.util.Map;

public abstract interface CTFOCacheTablePipeline {
    public abstract void add(String paramString1, String paramString2)
        throws DataCenterException;

    public abstract void delete(String paramString)
        throws DataCenterException;

    public abstract void commit()
        throws DataCenterException;

    public abstract void add(String paramString1, int paramInt, String paramString2)
        throws DataCenterException;

    public abstract List<Object> commitAndReturnAll()
        throws DataCenterException;

    public abstract void add(String paramString, int paramInt, byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract void add(String paramString, byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract void addRList(String paramString, List<String> paramList)
        throws DataCenterException;

    public abstract void addRList(String paramString, int paramInt, List<String> paramList)
        throws DataCenterException;

    public abstract void addRList(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract void addRList(String paramString, int paramInt, String... paramVarArgs)
        throws DataCenterException;

    public abstract void addRList(String paramString, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void addRList(String paramString, int paramInt, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void addLList(String paramString, List<String> paramList)
        throws DataCenterException;

    public abstract void addLList(String paramString, int paramInt, List<String> paramList)
        throws DataCenterException;

    public abstract void addLList(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract void addLList(String paramString, int paramInt, String... paramVarArgs)
        throws DataCenterException;

    public abstract void addLList(String paramString, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void addLList(String paramString, int paramInt, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void addSet(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract void addSet(String paramString, int paramInt, String... paramVarArgs)
        throws DataCenterException;

    public abstract void addSet(String paramString, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void addSet(String paramString, int paramInt, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void addSortedSet(String paramString1, double paramDouble, String paramString2)
        throws DataCenterException;

    public abstract void addSortedSet(String paramString1, int paramInt, double paramDouble, String paramString2)
        throws DataCenterException;

    public abstract void addSortedSet(String paramString, double paramDouble, byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract void addSortedSet(String paramString, int paramInt, double paramDouble, byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract void addHash(String paramString1, String paramString2, String paramString3)
        throws DataCenterException;

    public abstract void addHash(String paramString1, int paramInt, String paramString2, String paramString3)
        throws DataCenterException;

    public abstract void addHash(String paramString, byte[] paramArrayOfByte1, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract void addHash(String paramString, int paramInt, byte[] paramArrayOfByte1, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract void addHash(String paramString, Map<String, String> paramMap)
        throws DataCenterException;

    public abstract void addHash(String paramString, int paramInt, Map<String, String> paramMap)
        throws DataCenterException;

    public abstract void addHashByte(String paramString, Map<byte[], byte[]> paramMap)
        throws DataCenterException;

    public abstract void addHashByte(String paramString, int paramInt, Map<byte[], byte[]> paramMap)
        throws DataCenterException;

    public abstract void deleteHashField(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract void deleteHashField(String paramString, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void deleteSetValue(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract void deleteSetValue(String paramString, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void deleteSortedSetValue(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract void deleteSortedSetByScore(String paramString, double paramDouble1, double paramDouble2)
        throws DataCenterException;

    public abstract void deleteSortedSetByRank(String paramString, long paramLong1, long paramLong2)
        throws DataCenterException;

    public abstract void deleteSortedSetValue(String paramString, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void deleteListValue(String paramString1, long paramLong, String paramString2)
        throws DataCenterException;

    public abstract void deleteListValue(String paramString, int paramInt, byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract void queryTTL(String paramString)
        throws DataCenterException;

    public abstract void query(String paramString)
        throws DataCenterException;

    public abstract void queryByte(String paramString)
        throws DataCenterException;

    public abstract void queryList(String paramString)
        throws DataCenterException;

    public abstract void queryListByte(String paramString)
        throws DataCenterException;

    public abstract void queryRList(String paramString)
        throws DataCenterException;

    public abstract void queryRListByte(String paramString)
        throws DataCenterException;

    public abstract void queryLList(String paramString)
        throws DataCenterException;

    public abstract void queryLListByte(String paramString)
        throws DataCenterException;

    public abstract void popRList(String paramString)
        throws DataCenterException;

    public abstract void popRListByte(String paramString)
        throws DataCenterException;

    public abstract void popLList(String paramString)
        throws DataCenterException;

    public abstract void popLListByte(String paramString)
        throws DataCenterException;

    public abstract void queryListSize(String paramString)
        throws DataCenterException;

    public abstract void querySet(String paramString)
        throws DataCenterException;

    public abstract void querySetByte(String paramString)
        throws DataCenterException;

    public abstract void querySetSize(String paramString)
        throws DataCenterException;

    public abstract void querySortedSet(String paramString)
        throws DataCenterException;

    public abstract void querySortedSet(String paramString, long paramLong1, long paramLong2)
        throws DataCenterException;

    public abstract void querySortedSetByScore(String paramString, double paramDouble1, double paramDouble2)
        throws DataCenterException;

    public abstract void querySortedSetByte(String paramString)
        throws DataCenterException;

    public abstract void querySortedSetSize(String paramString)
        throws DataCenterException;

    public abstract void queryHash(String paramString)
        throws DataCenterException;

    public abstract void queryHashByte(String paramString)
        throws DataCenterException;

    public abstract void queryHashField(String paramString)
        throws DataCenterException;

    public abstract void queryHashFieldByte(String paramString)
        throws DataCenterException;

    public abstract void queryHash(String paramString1, String paramString2)
        throws DataCenterException;

    public abstract void queryHashByte(String paramString, byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract void queryHash(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract void queryHash(String paramString, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract void queryHashSize(String paramString)
        throws DataCenterException;

    public abstract void incr(String paramString)
        throws DataCenterException;

    public abstract void incr(String paramString, int paramInt)
        throws DataCenterException;

    public abstract void decr(String paramString)
        throws DataCenterException;

    public abstract void decr(String paramString, int paramInt)
        throws DataCenterException;

    public abstract void expire(String paramString, int paramInt)
        throws DataCenterException;

    public abstract void isSetValueExist(String paramString1, String paramString2)
        throws DataCenterException;

    public abstract void isKeyExist(String paramString)
        throws DataCenterException;

    public abstract void close()
        throws DataCenterException;
}
