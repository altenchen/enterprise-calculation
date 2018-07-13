package com.ctfo.datacenter.cache.command;

import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class CmdDataManager {
    private static final Logger logger = LoggerFactory.getLogger(CmdDataManager.class);

    public static void main(String[] args) {
        String configFile = null;
        String type = null;

        InputStream in = null;
        try {
            if (args.length == 2) {
                configFile = args[0];
                type = args[1];
            } else {
                System.out.println("CmdDataManager[error: param error  ]");
                return;
            }
            in = new BufferedInputStream(new FileInputStream(configFile));
            Properties p = new Properties();
            p.load(in);
            String configAddress = p.getProperty("common_config_virtualIPPort").trim();

            CmdDataManager cmdDataManager = new CmdDataManager();
            cmdDataManager.deal(configAddress, type);
            return;
        } catch (Exception e) {
            logger.error("CmdDataManager[error:" + e.getMessage() + "]");
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

    private void deal(String configAddress, String type)
        throws DataCenterException {
        StringBuffer sb = new StringBuffer();
        if ("flush".equals(type)) {
            String addresskey = RedisUtil.getConfigKey("address");
            String[] ipport = configAddress.split(":");
            Jedis j = new Jedis(ipport[0], Integer.parseInt(ipport[1]));
            Map<String, String> map = j.hgetAll(addresskey);
            j.close();

            sb.append("CmdDataManager[type:");
            sb.append(type);
            sb.append(",address:");
            for (String name : map.keySet()) {
                String address = (String) map.get(name);
                sb.append(address);
                sb.append(",");
                String[] ipports = address.split(":");
                Jedis jedis = new Jedis(ipports[0], Integer.parseInt(ipports[1]));
                jedis.flushAll();
                jedis.close();
            }
            sb.append("]");
        }
        logger.info(sb.toString());
        System.out.println(sb.toString());
    }
}
