package com.ctfo.datacenter.cache.config;

import com.ctfo.datacenter.cache.conn.ConfigControl;
import com.ctfo.datacenter.cache.conn.impl.ConfigControlImpl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CTFOInitTestCache {
    private static final Logger logger = LoggerFactory.getLogger(CTFOInitTestCache.class);

    public static void main(String[] args) {
        InputStream in = null;
        String configFile = null;
        try {
            if ((null != args) && (args.length > 0)) {
                configFile = args[0];
            } else {
                configFile = "DataCenter.xml";
            }
            in = new BufferedInputStream(new FileInputStream(configFile));
            Properties p = new Properties();
            p.load(in);

            CTFOInitTestCache ctfoInitCache = new CTFOInitTestCache();
            ctfoInitCache.init(p);
            return;
        } catch (Exception e) {
            logger.error("init cache[error:" + e.getMessage() + "]");
            System.out.println("init cache[Faild,Please initialized again]");
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

    private void init(Properties p)
        throws DataCenterException {
        CTFOConfigBean ctfoConfigBean = new CTFOConfigBean();
        List<CTFODataBean> list = new ArrayList();
        int i = 1;
        for (; ; ) {
            String common_data_virtualIPPort = p.getProperty("common_data_virtualIPPort_" + i);
            if ((common_data_virtualIPPort == null) || ("0.0.0.0:0000".equals(common_data_virtualIPPort))) {
                break;
            }
            CTFODataBean ctfoDataBean = new CTFODataBean();
            ctfoDataBean.setName(System.currentTimeMillis() + "_data_" + i);
            ctfoDataBean.setAddress(common_data_virtualIPPort);
            list.add(ctfoDataBean);
            i++;
        }
        if (list.size() > 0) {
            ctfoConfigBean.setAddress(p.getProperty("common_config_virtualIPPort").trim());
            ctfoConfigBean.setCtfoDataBean(list);
            dealInit(ctfoConfigBean);
        } else {
            String msg = "init cache[init cache error,please init config]";
            logger.info(msg);
            System.out.println(msg);
        }
    }

    public void dealInit(CTFOConfigBean ctfoConfigBean)
        throws DataCenterException {
        ConfigControl c = new ConfigControlImpl(ctfoConfigBean.getAddress());
        c.flushDB();

        String key = RedisUtil.getConfigKey("address");
        String oldkey = RedisUtil.getConfigKey("oldaddress");
        String shardstatuskey = RedisUtil.getConfigKey("shardchannelstatus");
        for (int i = 0; i < ctfoConfigBean.getCtfoDataBean().size(); i++) {
            CTFODataBean ctfoDataBean = (CTFODataBean) ctfoConfigBean.getCtfoDataBean().get(i);
            c.addHash(key, ctfoDataBean.getName(), ctfoDataBean.getAddress());
            c.addHash(oldkey, ctfoDataBean.getName(), ctfoDataBean.getAddress());
            StringBuffer sb = new StringBuffer();
            sb.append("init  test cache[ name:");
            sb.append(ctfoDataBean.getName());
            sb.append(",address:");
            sb.append(ctfoDataBean.getAddress());
            sb.append("(master:");
            sb.append(ctfoDataBean.getMaster_address());
            sb.append(",slave:");
            sb.append(ctfoDataBean.getSlave_address());
            sb.append(")]");
            System.out.println(sb.toString());
            logger.info(sb.toString());
        }
        c.add(shardstatuskey, "0");
    }
}
