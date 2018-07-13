package com.ctfo.datacenter.cache.handle;

import com.ctfo.datacenter.cache.conn.ConnConfig;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.impl.CTFODBManagerImpl;

public class DataCenter {
    public static CTFODBManager newCTFOInstance(String datatype, String address)
        throws DataCenterException {
        if ((datatype == null) || (address == null) || ("".equals(address))) {
            throw new DataCenterException("param error");
        }
        ConnConfig connConfig = new ConnConfig();

        CTFODBManager ctfoDBManager = null;
        if ("cache".equals(datatype)) {
            ctfoDBManager = new CTFODBManagerImpl(address, connConfig);
        } else {
            throw new DataCenterException("param error");
        }
        return ctfoDBManager;
    }

    public static CTFODBManager newCTFOInstance(String datatype, String address, int timeout)
        throws DataCenterException {
        if ((datatype == null) || (address == null) || ("".equals(address)) || (timeout <= 0)) {
            throw new DataCenterException("param error");
        }
        ConnConfig connConfig = new ConnConfig();
        connConfig.setSHARDPOOL_TIMEOUT(timeout * 1000);

        CTFODBManager ctfoDBManager = null;
        if ("cache".equals(datatype)) {
            ctfoDBManager = new CTFODBManagerImpl(address, connConfig);
        } else {
            throw new DataCenterException("param error");
        }
        return ctfoDBManager;
    }

    public static CTFODBManager newCTFOInstance(String datatype, String address, int maxactive, int maxidle, int maxwait)
        throws DataCenterException {
        if ((datatype == null) || (address == null) || ("".equals(address)) || (maxactive < maxidle) || (maxidle <= 0) || (maxwait <= 0)) {
            throw new DataCenterException("param error");
        }
        ConnConfig connConfig = new ConnConfig();
        connConfig.setSHARDPOOL_MAXACTIVE(maxactive);
        connConfig.setSHARDPOOL_MAXIDLE(maxidle);
        connConfig.setSHARDPOOL_MAXWAIT(maxwait * 1000);

        CTFODBManager ctfoDBManager = null;
        if ("cache".equals(datatype)) {
            ctfoDBManager = new CTFODBManagerImpl(address, connConfig);
        } else {
            throw new DataCenterException("param error");
        }
        return ctfoDBManager;
    }

    public static CTFODBManager newCTFOInstance(String datatype, String address, int timeout, int maxactive, int maxidle, int maxwait)
        throws DataCenterException {
        if ((datatype == null) || (address == null) || ("".equals(address)) || (timeout <= 0) || (maxactive < maxidle) || (maxidle <= 0) || (maxwait <= 0)) {
            throw new DataCenterException("param error");
        }
        ConnConfig connConfig = new ConnConfig();
        connConfig.setSHARDPOOL_TIMEOUT(timeout * 1000);
        connConfig.setSHARDPOOL_MAXACTIVE(maxactive);
        connConfig.setSHARDPOOL_MAXIDLE(maxidle);
        connConfig.setSHARDPOOL_MAXWAIT(maxwait * 1000);

        CTFODBManager ctfoDBManager = null;
        if ("cache".equals(datatype)) {
            ctfoDBManager = new CTFODBManagerImpl(address, connConfig);
        } else {
            throw new DataCenterException("param error");
        }
        return ctfoDBManager;
    }
}
