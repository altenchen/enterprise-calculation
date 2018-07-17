package com.ctfo.datacenter.cache.conn.base;

import com.ctfo.datacenter.cache.exception.DataCenterException;

public abstract interface DeleteCacheConn {
    public abstract Long delete(String paramString)
        throws DataCenterException;

    public abstract Long deleteHashField(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long deleteHashField(byte[] paramArrayOfByte, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long deleteSetValue(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long deleteSetValue(byte[] paramArrayOfByte, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long deleteSortedSetByScore(String paramString, double paramDouble1, double paramDouble2)
        throws DataCenterException;

    public abstract Long deleteSortedSetValue(String paramString, String... paramVarArgs)
        throws DataCenterException;

    public abstract Long deleteSortedSetValue(byte[] paramArrayOfByte, byte[]... paramVarArgs)
        throws DataCenterException;

    public abstract Long deleteListValue(String paramString1, long paramLong, String paramString2)
        throws DataCenterException;

    public abstract Long deleteListValue(byte[] paramArrayOfByte1, int paramInt, byte[] paramArrayOfByte2)
        throws DataCenterException;

    public abstract Long deleteSortedSetByRank(String paramString, long paramLong1, long paramLong2)
        throws DataCenterException;
}
