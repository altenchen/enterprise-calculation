package com.ctfo.datacenter.cache.conn.base;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.Map;

public abstract interface AddCacheConn {
    public abstract String add(String paramString1, String paramString2)
        throws DataCenterException;

    public abstract String add(byte[] paramArrayOfByte1, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract Long addRList(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long addRList(byte[] paramArrayOfByte, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long addLList(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long addLList(byte[] paramArrayOfByte, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long addSet(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long addSet(byte[] paramArrayOfByte, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long addSortedSet(String paramString1, double paramDouble, String paramString2)
        throws DataCenterException;

    public abstract Long addSortedSet(byte[] paramArrayOfByte1, double paramDouble, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract Long addHash(String paramString1, String paramString2, String paramString3)
        throws DataCenterException;

    public abstract Long addHash(byte[] paramArrayOfByte1, byte[] paramArrayOfByte2, byte[] paramArrayOfByte3)
        throws DataCenterException;

    public abstract String addHash(String paramString, Map<String, String> paramMap)
        throws DataCenterException;

    public abstract String addHash(byte[] paramArrayOfByte, Map<byte[], byte[]> paramMap)
        throws DataCenterException;
}
