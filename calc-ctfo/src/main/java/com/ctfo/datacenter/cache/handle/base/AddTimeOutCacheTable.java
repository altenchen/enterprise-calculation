package com.ctfo.datacenter.cache.handle.base;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.List;
import java.util.Map;

public abstract interface AddTimeOutCacheTable {
    public abstract String add(String paramString1, int paramInt, String paramString2)
        throws DataCenterException;

    public abstract String add(String paramString, int paramInt, byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract Long addRList(String paramString, int paramInt, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long addRList(String paramString, int paramInt, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long addLList(String paramString, int paramInt, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long addLList(String paramString, int paramInt, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long addSet(String paramString, int paramInt, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long addSet(String paramString, int paramInt, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long addSortedSet(String paramString1, int paramInt, double paramDouble, String paramString2)
        throws DataCenterException;

    public abstract Long addSortedSet(String paramString, int paramInt, double paramDouble, byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract Long addHash(String paramString1, int paramInt, String paramString2, String paramString3)
        throws DataCenterException;

    public abstract Long addHash(String paramString, int paramInt, byte[] paramArrayOfByte1, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract String addHash(String paramString, int paramInt, Map<String, String> paramMap)
        throws DataCenterException;

    public abstract String addHashByte(String paramString, int paramInt, Map<byte[], byte[]> paramMap)
        throws DataCenterException;

    public abstract Long addRList(String paramString, int paramInt, List<String> paramList)
        throws DataCenterException;

    public abstract Long addLList(String paramString, int paramInt, List<String> paramList)
        throws DataCenterException;

    public abstract Long expire(String paramString, int paramInt)
        throws DataCenterException;
}
