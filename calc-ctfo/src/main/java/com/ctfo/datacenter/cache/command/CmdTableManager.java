package com.ctfo.datacenter.cache.command;

import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOCacheDB;
import com.ctfo.datacenter.cache.handle.CTFODBManager;
import com.ctfo.datacenter.cache.handle.DataCenter;
import com.ctfo.datacenter.cache.handle.impl.CTFOCacheDBImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CmdTableManager {
    private static final Logger logger = LoggerFactory.getLogger(CmdTableManager.class);

    public static void main(String[] args) {
        String configFile = null;
        String type = null;
        String dbname = null;
        String tablename = null;
        InputStream in = null;
        try {
            if (args.length == 4) {
                configFile = args[0];
                type = args[1];
                dbname = args[2];
                tablename = args[3];
            } else {
                System.out.println("CmdTableManager[error: param error]");
                return;
            }
            in = new BufferedInputStream(new FileInputStream(configFile));
            Properties p = new Properties();
            p.load(in);
            String configAddress = p.getProperty("common_config_virtualIPPort").trim();

            CmdTableManager cmdDBManager = new CmdTableManager();
            cmdDBManager.deal(configAddress, type, dbname, tablename);
            return;
        } catch (Exception e) {
            logger.error("CmdTableManager[error:" + e.getMessage() + "]");
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void deal(String configAddress, String type, String dbname, String tablename)
        throws DataCenterException {
        long result = 0L;
        String msg = "";
        CTFODBManager ctfoDBManager = DataCenter.newCTFOInstance("cache", configAddress);
        boolean flag = ctfoDBManager.isDBExist(dbname).booleanValue();
        if (!flag) {
            StringBuffer sb = new StringBuffer();
            sb.append("CmdTableManager[type:");
            sb.append(type);
            sb.append(",fail:dbname-");
            sb.append(dbname);
            sb.append(" is not exist]");
            logger.info(sb.toString());
            System.out.println(sb.toString());
            return;
        }
        CTFOCacheDB ctfoCacheDB = ctfoDBManager.openCacheDB(dbname);
        CTFOCacheDBImpl manager = (CTFOCacheDBImpl) ctfoCacheDB;
        if ("add".equals(type)) {
            result = manager.addTable(tablename);
            if (result == 0L) {
                msg = "fail:table is exist";
            } else {
                msg = "success:" + result + " table change";
            }
        } else if ("delete".equals(type)) {
            result = manager.deleteTable(tablename);
            if (result == -1L) {
                msg = "fail:table is not exist";
            } else {
                msg = "success:" + result + " data change";
            }
        }
        StringBuffer sb = new StringBuffer();
        sb.append("CmdTableManager[type:");
        sb.append(type);
        sb.append(",dbname:");
        sb.append(dbname);
        sb.append(",tablename:");
        sb.append(tablename);
        sb.append(",");
        sb.append(msg);
        sb.append("]");
        logger.info(sb.toString());
        System.out.println(sb.toString());
    }
}
