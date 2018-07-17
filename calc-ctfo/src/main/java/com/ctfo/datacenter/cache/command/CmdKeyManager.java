package com.ctfo.datacenter.cache.command;

import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOCacheDB;
import com.ctfo.datacenter.cache.handle.CTFOCacheTable;
import com.ctfo.datacenter.cache.handle.CTFODBManager;
import com.ctfo.datacenter.cache.handle.DataCenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CmdKeyManager {
    private static final Logger logger = LoggerFactory.getLogger(CmdKeyManager.class);

    public static void main(String[] args) {
        String configFile = null;
        String type = null;
        String dbname = null;
        String tablename = null;
        String key = null;
        InputStream in = null;
        try {
            if (args.length == 5) {
                configFile = args[0];
                type = args[1];
                dbname = args[2];
                tablename = args[3];
                key = args[4];
            } else {
                System.out.println("CmdKeyManager[error: param error]");
                return;
            }
            in = new BufferedInputStream(new FileInputStream(configFile));
            Properties p = new Properties();
            p.load(in);
            String configAddress = p.getProperty("common_config_virtualIPPort").trim();

            CmdKeyManager cmdKeyManager = new CmdKeyManager();
            cmdKeyManager.deal(configAddress, type, dbname, tablename, key);
            return;
        } catch (Exception e) {
            logger.error("CmdKeyManager[error:" + e.getMessage() + "]");
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

    private void deal(String configAddress, String type, String dbname, String tablename, String key)
        throws DataCenterException {
        if ("ttl".equals(type)) {
            CTFODBManager ctfoDBManager = DataCenter.newCTFOInstance("cache", configAddress);
            boolean flag = ctfoDBManager.isDBExist(dbname).booleanValue();
            if (!flag) {
                StringBuffer sb = new StringBuffer();
                sb.append("CmdKeyManager[type:");
                sb.append(type);
                sb.append(",fail:dbname-");
                sb.append(dbname);
                sb.append(" is not exist]");
                logger.info(sb.toString());
                System.out.println(sb.toString());
                return;
            }
            CTFOCacheDB ctfoCacheDB = ctfoDBManager.openCacheDB(dbname);
            boolean flag2 = ctfoCacheDB.isTableExist(tablename).booleanValue();
            if (!flag2) {
                StringBuffer sb = new StringBuffer();
                sb.append("CmdKeyManager[type:");
                sb.append(type);
                sb.append(",fail:tablename-");
                sb.append(tablename);
                sb.append(" is not exist]");
                logger.info(sb.toString());
                System.out.println(sb.toString());
                return;
            }
            CTFOCacheTable ctfoCacheTable = ctfoCacheDB.getTable(tablename);
            long ttl = ctfoCacheTable.queryTTL(key).longValue();
            if (ttl == -1L) {
                StringBuffer sb = new StringBuffer();
                sb.append("CmdKeyManager[type:");
                sb.append(type);
                sb.append(",key-");
                sb.append(key);
                sb.append(" is no timeout]");
                logger.info(sb.toString());
                System.out.println(sb.toString());
                return;
            }
            if (ttl == -2L) {
                StringBuffer sb = new StringBuffer();
                sb.append("CmdKeyManager[type:");
                sb.append(type);
                sb.append(",key-");
                sb.append(key);
                sb.append(" is not exist]");
                logger.info(sb.toString());
                System.out.println(sb.toString());
                return;
            }
            StringBuffer sb = new StringBuffer();
            sb.append("CmdKeyManager[type:");
            sb.append(type);
            sb.append(",dbname:");
            sb.append(dbname);
            sb.append(",tablename:");
            sb.append(tablename);
            sb.append(",key:");
            sb.append(key);
            sb.append(",time remaining(s):");
            sb.append(ttl);
            sb.append("]");
            logger.info(sb.toString());
            System.out.println(sb.toString());
        } else {
            System.out.println("CmdKeyManager[error: param error]");
            logger.info("CmdKeyManager[error: param error]");
        }
    }
}
