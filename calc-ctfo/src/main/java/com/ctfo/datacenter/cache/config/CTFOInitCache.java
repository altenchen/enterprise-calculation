package com.ctfo.datacenter.cache.config;

import com.ctfo.datacenter.cache.conn.ConfigControl;
import com.ctfo.datacenter.cache.conn.impl.ConfigControlImpl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CTFOInitCache {
    private static final Logger logger = LoggerFactory.getLogger(CTFOInitCache.class);
    String[] types = null;

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

            CTFOInitCache ctfoInitCache = new CTFOInitCache();
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
        String type = p.getProperty("common_area_type");
        if (type != null) {
            this.types = type.split(",");
        }
        for (; ; ) {
            String common_data_virtualIPPort = p.getProperty("common_data_virtualIPPort_" + i);
            if ((common_data_virtualIPPort == null) || ("0.0.0.0:0000".equals(common_data_virtualIPPort))) {
                break;
            }
            String common_data_masterIPPort = p.getProperty("common_data_masterIPPort_" + i).trim();
            String common_data_slaveIPPort = p.getProperty("common_data_slaveIPPort_" + i).trim();
            CTFODataBean ctfoDataBean = new CTFODataBean();
            if (this.types != null) {
                String[] s = new String[this.types.length];
                for (int j = 0; j < s.length; j++) {
                    s[j] = p.getProperty("common_data_" + this.types[j] + "IPPort_" + i).trim();
                }
                ctfoDataBean.setArea(s);
            }
            ctfoDataBean.setName(System.currentTimeMillis() + "_data_" + i);
            ctfoDataBean.setAddress(common_data_virtualIPPort.trim());
            if ("0.0.0.0:0000".equals(common_data_masterIPPort)) {
                ctfoDataBean.setMaster_address("-");
            } else {
                ctfoDataBean.setMaster_address(common_data_masterIPPort);
            }
            if ("0.0.0.0:0000".equals(common_data_slaveIPPort)) {
                ctfoDataBean.setSlave_address("-");
            } else {
                ctfoDataBean.setSlave_address(common_data_slaveIPPort);
            }
            list.add(ctfoDataBean);
            i++;
        }
        if (list.size() > 0) {
            ctfoConfigBean.setAddress(p.getProperty("common_config_virtualIPPort").trim());
            String common_config_masterIPPort = p.getProperty("common_config_masterIPPort").trim();
            String common_config_slaveIPPort = p.getProperty("common_config_slaveIPPort").trim();
            if ("0.0.0.0:0000".equals(common_config_masterIPPort)) {
                ctfoConfigBean.setMaster_address("-");
            } else {
                ctfoConfigBean.setMaster_address(common_config_masterIPPort);
            }
            if ("0.0.0.0:0000".equals(common_config_slaveIPPort)) {
                ctfoConfigBean.setSlave_address("-");
            } else {
                ctfoConfigBean.setSlave_address(common_config_slaveIPPort);
            }
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
        String monitorkey = RedisUtil.getConfigKey("mon", "address");

        List<String> addrlist = new ArrayList();
        if ((ctfoConfigBean.getMaster_address() != null) && (ctfoConfigBean.getMaster_address().length() > 10)) {
            String[] ip_port = ctfoConfigBean.getMaster_address().split(":");
            Jedis jedis = new Jedis(ip_port[0], Integer.parseInt(ip_port[1]));
            jedis.ping();
            jedis.disconnect();
            addrlist.add(ctfoConfigBean.getMaster_address());

            String config_masterkey = RedisUtil.getConfigKey("mon", ctfoConfigBean.getMaster_address());
            c.addHash(config_masterkey, "addr", ctfoConfigBean.getMaster_address());
            c.addHash(config_masterkey, "type", "0");
            c.addHash(config_masterkey, "status", "0");
            c.addHash(config_masterkey, "cpu", ctfoConfigBean.getCpu());
            c.addHash(config_masterkey, "mem", ctfoConfigBean.getMem());
            c.addHash(config_masterkey, "io", ctfoConfigBean.getIo());
        } else {
            addrlist.add("-");
        }
        if ((ctfoConfigBean.getSlave_address() != null) && (ctfoConfigBean.getSlave_address().length() > 10)) {
            String[] ip_port = ctfoConfigBean.getSlave_address().split(":");
            Jedis jedis = new Jedis(ip_port[0], Integer.parseInt(ip_port[1]));
            jedis.ping();
            jedis.disconnect();
            addrlist.add(ctfoConfigBean.getSlave_address());

            String config_slavekey = RedisUtil.getConfigKey("mon", ctfoConfigBean.getSlave_address());
            c.addHash(config_slavekey, "addr", ctfoConfigBean.getSlave_address());
            c.addHash(config_slavekey, "type", "0");
            c.addHash(config_slavekey, "status", "1");
            c.addHash(config_slavekey, "cpu", ctfoConfigBean.getCpu());
            c.addHash(config_slavekey, "mem", ctfoConfigBean.getMem());
            c.addHash(config_slavekey, "io", ctfoConfigBean.getIo());
        } else {
            addrlist.add("-");
        }
        for (int i = 0; i < ctfoConfigBean.getCtfoDataBean().size(); i++) {
            CTFODataBean ctfoDataBean = (CTFODataBean) ctfoConfigBean.getCtfoDataBean().get(i);
            if ((ctfoDataBean.getMaster_address() != null) && (ctfoDataBean.getMaster_address().length() > 10)) {
                String[] ip_port = ctfoDataBean.getMaster_address().split(":");
                Jedis jedis = new Jedis(ip_port[0], Integer.parseInt(ip_port[1]));
                jedis.ping();
                jedis.disconnect();
                addrlist.add(ctfoDataBean.getMaster_address());

                String masterkey = RedisUtil.getConfigKey("mon", ctfoDataBean.getMaster_address());
                c.addHash(masterkey, "addr", ctfoDataBean.getMaster_address());
                c.addHash(masterkey, "name", ctfoDataBean.getName());
                c.addHash(masterkey, "type", i + 1 + "");
                c.addHash(masterkey, "status", "0");
                c.addHash(masterkey, "cpu", ctfoDataBean.getCpu());
                c.addHash(masterkey, "mem", ctfoDataBean.getMem());
                c.addHash(masterkey, "io", ctfoDataBean.getIo());
            } else {
                addrlist.add("-");
            }
            if ((ctfoDataBean.getSlave_address() != null) && (ctfoDataBean.getSlave_address().length() > 10)) {
                String[] ip_port = ctfoDataBean.getSlave_address().split(":");
                Jedis jedis = new Jedis(ip_port[0], Integer.parseInt(ip_port[1]));
                jedis.ping();
                jedis.disconnect();
                addrlist.add(ctfoDataBean.getSlave_address());

                String slavekey = RedisUtil.getConfigKey("mon", ctfoDataBean.getSlave_address());
                c.addHash(slavekey, "addr", ctfoDataBean.getSlave_address());
                c.addHash(slavekey, "name", ctfoDataBean.getName());
                c.addHash(slavekey, "type", i + 1 + "");
                c.addHash(slavekey, "status", "1");
                c.addHash(slavekey, "cpu", ctfoDataBean.getCpu());
                c.addHash(slavekey, "mem", ctfoDataBean.getMem());
                c.addHash(slavekey, "io", ctfoDataBean.getIo());
            } else {
                addrlist.add("-");
            }
            c.addHash(key, ctfoDataBean.getName(), ctfoDataBean.getAddress());
            c.addHash(oldkey, ctfoDataBean.getName(), ctfoDataBean.getAddress());
            if (this.types != null) {
                for (int j = 0; j < this.types.length; j++) {
                    c.addHash(key + "-" + this.types[j], ctfoDataBean.getName(), ctfoDataBean.getArea()[j]);
                    c.addHash(oldkey + "-" + this.types[j], ctfoDataBean.getName(), ctfoDataBean.getArea()[j]);
                }
            }
            StringBuffer sb = new StringBuffer();
            sb.append("init cache[add name:");
            sb.append(ctfoDataBean.getName());
            sb.append(",address:");
            sb.append(ctfoDataBean.getAddress());
            sb.append("(master:");
            sb.append(ctfoDataBean.getMaster_address());
            sb.append(",slave:");
            sb.append(ctfoDataBean.getSlave_address());
            sb.append(")]");
            logger.info(sb.toString());
        }
        c.addRList(monitorkey, (String[]) addrlist.toArray(new String[addrlist.size()]));

        c.add(shardstatuskey, "0");

        Map<String, String> map = c.queryHash(key);
        List<String> list = c.queryList(monitorkey, Long.valueOf(0L), Long.valueOf(-1L));

        StringBuffer sb = new StringBuffer();
        sb.append("init cache[finish:");
        sb.append(map);
        sb.append(",area:");
        if (this.types != null) {
            for (int j = 0; j < this.types.length; j++) {
                sb.append(c.queryHash(key + "-" + this.types[j]));
                sb.append(",");
            }
        }
        sb.append("monitor:");
        sb.append(list);
        sb.append("]");
        logger.info(sb.toString());
        System.out.println(sb.toString());
    }
}
