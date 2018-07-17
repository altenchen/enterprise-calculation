package com.ctfo.datacenter.cache.command;

import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFODBManager;
import com.ctfo.datacenter.cache.handle.DataCenter;
import com.ctfo.datacenter.cache.handle.impl.CTFODBManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CmdDBManager {
    private static final Logger logger = LoggerFactory.getLogger(CmdDBManager.class);

    public static void main(String[] args) {
        String configFile = null;
        String type = null;
        String value = null;
        InputStream in = null;
        try {
            if (args.length == 3) {
                configFile = args[0];
                type = args[1];
                value = args[2];
            } else {
                System.out.println("CmdDBManager[error: param error  ]");
                return;
            }
            in = new BufferedInputStream(new FileInputStream(configFile));
            Properties p = new Properties();
            p.load(in);
            String configAddress = p.getProperty("common_config_virtualIPPort").trim();

            CmdDBManager cmdDBManager = new CmdDBManager();
            cmdDBManager.deal(configAddress, type, value);
            return;
        } catch (Exception e) {
            logger.error("CmdDBManager[error:" + e.getMessage() + "]");
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

    private void deal(String configAddress, String type, String value)
        throws DataCenterException {
        long result = 0L;
        String msg = "";
        CTFODBManager ctfoDBManager = DataCenter.newCTFOInstance("cache", configAddress);
        CTFODBManagerImpl manager = (CTFODBManagerImpl) ctfoDBManager;
        if ("add".equals(type)) {
            result = manager.addDB(value);
            if (result == 0L) {
                msg = "fail:db is exist";
            } else {
                msg = "success:" + result + " db change";
            }
        } else if ("delete".equals(type)) {
            result = manager.deleteDB(value);
            if (result == -1L) {
                msg = "fail:db is not exist";
            } else {
                msg = "success:" + result + " table change";
            }
        } else if ("tablesize".equals(type)) {
            result = manager.getTableSize(value);
            if (result == -1L) {
                msg = "fail:db is not exist";
            } else {
                msg = "success:" + result + " tables";
            }
        }
        StringBuffer sb = new StringBuffer();
        sb.append("CmdDBManager[type:");
        sb.append(type);
        sb.append(",dbname:");
        sb.append(value);
        sb.append(",");
        sb.append(msg);
        sb.append("]");
        logger.info(sb.toString());
        System.out.println(sb.toString());
    }
}
