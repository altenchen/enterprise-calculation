package com.ctfo.datacenter.cache.handle.base;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.List;
import java.util.Map;

public abstract interface AddCacheTable {
    public abstract String add(String paramString1, String paramString2)
        throws DataCenterException;

    public abstract String add(String paramString, byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract Long addRList(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long addRList(String paramString, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long addLList(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long addLList(String paramString, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long addSet(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long addSet(String paramString, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long addSortedSet(String paramString1, double paramDouble, String paramString2)
        throws DataCenterException;

    public abstract Long addSortedSet(String paramString, double paramDouble, byte[] paramArrayOfByte)
        throws DataCenterException;

    public abstract Long addHash(String paramString1, String paramString2, String paramString3)
        throws DataCenterException;

    public abstract Long addHash(String paramString, byte[] paramArrayOfByte1, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract String addHash(String paramString, Map<String, String> paramMap)
        throws DataCenterException;

    public abstract String addHashByte(String paramString, Map<byte[], byte[]> paramMap)
        throws DataCenterException;

    public abstract Long addRList(String paramString, List<String> paramList)
        throws DataCenterException;

    public abstract Long addLList(String paramString, List<String> paramList)
        throws DataCenterException;
}
