package com.ctfo.datacenter.cache.conn.base;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.Map;

public abstract interface AddTimeOutCacheConn {
    public abstract String add(String paramString1, int paramInt, String paramString2)
        throws DataCenterException;

    public abstract String add(byte[] paramArrayOfByte1, int paramInt, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract Long addRList(String paramString, int paramInt, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long addRList(byte[] paramArrayOfByte, int paramInt, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long addLList(String paramString, int paramInt, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long addLList(byte[] paramArrayOfByte, int paramInt, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long addSet(String paramString, int paramInt, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long addSet(byte[] paramArrayOfByte, int paramInt, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long addSortedSet(String paramString1, int paramInt, double paramDouble, String paramString2)
        throws DataCenterException;

    public abstract Long addSortedSet(byte[] paramArrayOfByte1, int paramInt, double paramDouble, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract Long addHash(String paramString1, int paramInt, String paramString2, String paramString3)
        throws DataCenterException;

    public abstract Long addHash(byte[] paramArrayOfByte1, int paramInt, byte[] paramArrayOfByte2, byte[] paramArrayOfByte3)
        throws DataCenterException;

    public abstract String addHash(String paramString, int paramInt, Map<String, String> paramMap)
        throws DataCenterException;

    public abstract String addHash(byte[] paramArrayOfByte, int paramInt, Map<byte[], byte[]> paramMap)
        throws DataCenterException;

    public abstract Long expire(String paramString, int paramInt)
        throws DataCenterException;
}
