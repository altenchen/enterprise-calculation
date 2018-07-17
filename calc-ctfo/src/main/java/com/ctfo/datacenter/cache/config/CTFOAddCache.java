package com.ctfo.datacenter.cache.config;

import com.ctfo.datacenter.cache.conn.ConfigControl;
import com.ctfo.datacenter.cache.conn.ShardControl;
import com.ctfo.datacenter.cache.conn.impl.ConfigControlImpl;
import com.ctfo.datacenter.cache.conn.impl.ShardControlImpl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class CTFOAddCache {
    private static final Logger logger = LoggerFactory.getLogger(CTFOAddCache.class);
    String addresskey;
    String oldaddresskey;
    String shardstatuskey;
    ConfigControl c;
    Map<String, String> map;
    Map<String, String> areamap;
    ShardControl addShardedControl = null;
    static String status;
    int threadStatus = 0;
    String msg;
    CTFOConfigBean ctfoConfigBean;
    String[] types = null;

    public static void main(String[] args) {
        String configFile = null;
        InputStream in = null;
        try {
            if (args.length > 0) {
                configFile = args[0];
                status = args[1];
            } else {
                configFile = "DataCenter.xml";
            }
            in = new BufferedInputStream(new FileInputStream(configFile));
            Properties p = new Properties();
            p.load(in);

            CTFOAddCache ctfoAddCache = new CTFOAddCache();
            ctfoAddCache.init(p);
            return;
        } catch (Exception e) {
            logger.error("add cache[error:" + e.getMessage() + "]");
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
        throws InterruptedException, IOException, DataCenterException {
        CTFOConfigBean ctfoConfigBean = new CTFOConfigBean();
        List<CTFODataBean> list = new ArrayList();
        String type = p.getProperty("common_area_type");
        if (type != null) {
            this.types = type.split(",");
        }
        int i = 1;
        for (; ; ) {
            String common_add_virtualIPPort = p.getProperty("common_add_virtualIPPort_" + i);
            if ((common_add_virtualIPPort == null) || ("0.0.0.0:0000".equals(common_add_virtualIPPort))) {
                break;
            }
            String common_add_masterIPPort = p.getProperty("common_add_masterIPPort_" + i).trim();
            String common_add_slaveIPPort = p.getProperty("common_add_slaveIPPort_" + i).trim();
            CTFODataBean ctfoDataBean = new CTFODataBean();
            if (this.types != null) {
                String[] s = new String[this.types.length];
                for (int j = 0; j < s.length; j++) {
                    s[j] = p.getProperty("common_add_" + this.types[j] + "IPPort_" + i).trim();
                }
                ctfoDataBean.setArea(s);
            }
            ctfoDataBean.setName(System.currentTimeMillis() + "_add_" + i);
            ctfoDataBean.setAddress(common_add_virtualIPPort);
            if ("0.0.0.0:0000".equals(common_add_masterIPPort)) {
                ctfoDataBean.setMaster_address("-");
            } else {
                ctfoDataBean.setMaster_address(common_add_masterIPPort);
            }
            if ("0.0.0.0:0000".equals(common_add_slaveIPPort)) {
                ctfoDataBean.setSlave_address("-");
            } else {
                ctfoDataBean.setSlave_address(common_add_slaveIPPort);
            }
            list.add(ctfoDataBean);
            i++;
        }
        if (list.size() > 0) {
            ctfoConfigBean.setAddress(p.getProperty("common_config_virtualIPPort").trim());
            ctfoConfigBean.setMaster_address(p.getProperty("common_config_masterIPPort").trim());
            ctfoConfigBean.setSlave_address(p.getProperty("common_config_slaveIPPort").trim());
            ctfoConfigBean.setMoveThread(Integer.parseInt(p.getProperty("common_add_moveThread").trim()));
            ctfoConfigBean.setDeleteThread(Integer.parseInt(p.getProperty("common_add_deleteThread").trim()));
            ctfoConfigBean.setCtfoDataBean(list);
            this.ctfoConfigBean = ctfoConfigBean;
            dealUpdate();
        } else {
            String msg = "add cache[add cache error,please init config]";
            logger.info(msg);
            System.out.println(msg);
        }
    }

    public int init(CTFOConfigBean ctfoConfigBean)
        throws InterruptedException, IOException, DataCenterException {
        this.ctfoConfigBean = ctfoConfigBean;
        int returnvalue = dealUpdate();
        return returnvalue;
    }

    private int dealUpdate()
        throws InterruptedException, IOException, DataCenterException {
        int returnvalue = 0;
        this.addresskey = RedisUtil.getConfigKey("address");
        this.oldaddresskey = RedisUtil.getConfigKey("oldaddress");
        this.shardstatuskey = RedisUtil.getConfigKey("shardchannelstatus");

        this.c = new ConfigControlImpl(this.ctfoConfigBean.getAddress());
        for (int i = 0; i < this.ctfoConfigBean.getCtfoDataBean().size(); i++) {
            CTFODataBean ctfoDataBean = (CTFODataBean) this.ctfoConfigBean.getCtfoDataBean().get(i);
            if (ctfoDataBean.getMaster_address().equals(ctfoDataBean.getSlave_address())) {
                logger.info("master_address=slave_address");
            }
            if (!"-".equals(ctfoDataBean.getAddress())) {
                String[] ip_port1 = ctfoDataBean.getAddress().split(":");
                Jedis jedis1 = new Jedis(ip_port1[0], Integer.parseInt(ip_port1[1]));
                jedis1.ping();
                jedis1.disconnect();
            }
            if (!"-".equals(ctfoDataBean.getMaster_address())) {
                String[] ip_port2 = ctfoDataBean.getMaster_address().split(":");
                Jedis jedis2 = new Jedis(ip_port2[0], Integer.parseInt(ip_port2[1]));
                jedis2.ping();
                jedis2.disconnect();
            }
            if (!"-".equals(ctfoDataBean.getSlave_address())) {
                String[] ip_port3 = ctfoDataBean.getSlave_address().split(":");
                Jedis jedis3 = new Jedis(ip_port3[0], Integer.parseInt(ip_port3[1]));
                jedis3.ping();
                jedis3.disconnect();
            }
        }
        String monitorkey = RedisUtil.getConfigKey("mon", "address");
        List<String> addrlist = this.c.queryList(monitorkey, Long.valueOf(0L), Long.valueOf(-1L));
        for (int i = 0; i < addrlist.size(); i++) {
            if (!"-".equals((String) addrlist.get(i))) {
                for (int j = 0; j < this.ctfoConfigBean.getCtfoDataBean().size(); j++) {
                    CTFODataBean ctfoDataBean = (CTFODataBean) this.ctfoConfigBean.getCtfoDataBean().get(j);
                    if (((String) addrlist.get(i)).equals(ctfoDataBean.getAddress())) {
                        logger.info("address return 4");
                        return 4;
                    }
                    if (((String) addrlist.get(i)).equals(ctfoDataBean.getMaster_address())) {
                        logger.info("master_address return 4");
                        return 4;
                    }
                    if (((String) addrlist.get(i)).equals(ctfoDataBean.getSlave_address())) {
                        logger.info("slave_address return 4");
                        return 4;
                    }
                }
            }
        }
        String shardstatus = this.c.query(this.shardstatuskey);
        if (shardstatus == null) {
            logger.info("add cache[shardstatus is null,upgrade to stop]");
            return 1;
        }
        if (!"0".equals(shardstatus)) {
            if ("continue".equals(status)) {
                logger.info("add cache[user input :" + status + "]");
            } else {
                logger.info("add cache[a script in the operation or the last upgrade interrupt, enter \"sh CTFOAddCache.sh continue\"  to continue]");
                System.out.println("add cache[a script in the operation or the last upgrade interrupt, enter \"sh CTFOAddCache.sh continue\"  to continue]");

                logger.info("add cache[user input :" + status + ",upgrade to stop]");
                return 2;
            }
        }
        logger.info("add cache[shardStatus " + shardstatus + "]");
        this.map = this.c.queryHash(this.addresskey);
        Map<String, String> oldmap = this.c.queryHash(this.oldaddresskey);

        ShardControl shardedControl = new ShardControlImpl(oldmap);

        int status = Integer.parseInt(shardstatus);
        switch (status) {
            case 0:
                break;
            case 1:
                this.addShardedControl = new ShardControlImpl(this.map);
                break;
            case 2:
                this.addShardedControl = new ShardControlImpl(this.map);
                break;
            case 3:
                this.addShardedControl = new ShardControlImpl(this.map);
                break;
            case 4:
                this.addShardedControl = new ShardControlImpl(this.map);
                break;
            case 5:
                this.addShardedControl = new ShardControlImpl(this.map);
                break;
            default:
                logger.info("add cache[shardstatus error]");
                return 3;
        }
        if (addAddress()) {
            this.c.add(this.shardstatuskey, "1");
        } else {
            return 4;
        }
        this.c.publish("shardchannel", "1");
        this.c.add(this.shardstatuskey, "2");
        logger.info("add cache[wait 10 second]");
        waitClient(10);

        collect(shardedControl);
        this.c.add(this.shardstatuskey, "3");

        move();
        this.c.add(this.shardstatuskey, "4");

        this.c.publish("shardchannel", "0");
        this.c.add(this.shardstatuskey, "5");
        logger.info("add cache[wait 10 second]");
        waitClient(10);

        deletedata();
        modifyMonitorData();

        Map<String, String> map = this.c.queryHash(this.addresskey);
        this.c.addHash(this.oldaddresskey, map);
        if (this.types != null) {
            for (int j = 0; j < this.types.length; j++) {
                Map<String, String> areamap = this.c.queryHash(this.addresskey + "-" + this.types[j]);
                this.c.addHash(this.oldaddresskey + "-" + this.types[j], areamap);
            }
        }
        this.c.add(this.shardstatuskey, "0");

        Map<String, String> m = this.c.queryHash(this.addresskey);
        String msg = "add cache[finish" + m + "]";
        logger.info(msg);
        System.out.println(msg);

        return returnvalue;
    }

    private boolean addAddress()
        throws DataCenterException {
        logger.info("add cache[add address deal start]");

        Map<String, String> tempaddressmap = new TreeMap();
        boolean flag = false;
        for (int i = 0; i < this.ctfoConfigBean.getCtfoDataBean().size(); i++) {
            CTFODataBean ctfoDataBean = (CTFODataBean) this.ctfoConfigBean.getCtfoDataBean().get(i);
            if (this.map.get(ctfoDataBean.getName()) == null) {
                boolean addressflag = true;
                for (String existname : this.map.keySet()) {
                    String existaddress = (String) this.map.get(existname);
                    if (ctfoDataBean.getAddress().equals(existaddress)) {
                        addressflag = false;
                        break;
                    }
                }
                if (addressflag) {
                    flag = true;
                    tempaddressmap.put(ctfoDataBean.getName(), ctfoDataBean.getAddress());
                    logger.info("add cache[name:" + ctfoDataBean.getName() + ",address:" + ctfoDataBean.getAddress() + "]");
                } else {
                    logger.info("add cache[address:" + ctfoDataBean.getAddress() + " is exist]");
                }
            } else {
                logger.info("add cache[name:" + ctfoDataBean.getName() + " is exist]");
            }
        }
        if (!flag) {
            this.c.add(this.shardstatuskey, "0");
            logger.info("add cache[no add cache]");
            return false;
        }
        this.map.putAll(tempaddressmap);
        this.addShardedControl = new ShardControlImpl(this.map);
        this.c.addHash(this.addresskey, this.map);
        if (this.types != null) {
            for (int j = 0; j < this.types.length; j++) {
                for (int i = 0; i < this.ctfoConfigBean.getCtfoDataBean().size(); i++) {
                    this.c.addHash(this.addresskey + "-" + this.types[j], ((CTFODataBean) this.ctfoConfigBean.getCtfoDataBean().get(i)).getName(), ((CTFODataBean) this.ctfoConfigBean.getCtfoDataBean().get(i)).getArea()[j]);
                }
            }
        }
        logger.info("add cache[add address deal finish]");
        return true;
    }

    private void collect(ShardControl shardedControl)
        throws InterruptedException, DataCenterException {
        logger.info("add cache[collect key deal start]");

        Collection<Jedis> col = shardedControl.queryAllShards();

        CountDownLatch latch = new CountDownLatch(col.size());
        int i = 0;
        for (Jedis jedis : col) {
            CTFOAddCacheCollectThread t = new CTFOAddCacheCollectThread(i, this.ctfoConfigBean.getAddress(), jedis, shardedControl, this.addShardedControl, latch);
            t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    CTFOAddCache.this.threadStatus = 1;
                    CTFOAddCache.this.msg = (t.getName() + " : " + e.getMessage());
                }
            });
            t.start();
            i++;
        }
        latch.await();
        if (this.threadStatus == 1) {
            DataCenterException dataCenterException = new DataCenterException(this.msg);
            throw dataCenterException;
        }
        logger.info("add cache[collect key deal finish]");
    }

    private void move()
        throws InterruptedException, DataCenterException {
        logger.info("add cache[move key  deal start]");
        String pattern = RedisUtil.getConfigPatternKey("upgrade");
        Set<String> keys = this.c.keys(pattern);

        StringBuffer sb = new StringBuffer();
        sb.append("pattern:");
        sb.append(pattern);
        sb.append(",keys:");
        sb.append(keys);
        sb.append(",moveThread:");
        sb.append(this.ctfoConfigBean.getMoveThread());
        logger.info(sb.toString());
        for (String tk : keys) {
            String addr = tk.split("-")[2];

            CountDownLatch latch = new CountDownLatch(this.ctfoConfigBean.getMoveThread());
            for (int i = 0; i < this.ctfoConfigBean.getMoveThread(); i++) {
                CTFOAddCacheMoveThread t = new CTFOAddCacheMoveThread(i, this.ctfoConfigBean.getAddress(), tk, addr, this.map, latch);
                t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        CTFOAddCache.this.threadStatus = 1;
                        CTFOAddCache.this.msg = (t.getName() + " : " + e.getMessage());
                    }
                });
                t.start();
            }
            latch.await();
            if (this.threadStatus == 1) {
                DataCenterException dataCenterException = new DataCenterException(this.msg);
                throw dataCenterException;
            }
            this.c.delete(tk);
            logger.info("add cache[move key " + addr + " deal finish]");
        }
    }

    private void deletedata()
        throws InterruptedException, DataCenterException {
        logger.info("add cache[delete key  deal start]");
        String pattern = RedisUtil.getConfigPatternKey("delete");
        Set<String> keys = this.c.keys(pattern);
        for (String tk : keys) {
            String addr = tk.split("-")[2];

            CountDownLatch latch = new CountDownLatch(this.ctfoConfigBean.getDeleteThread());
            for (int i = 0; i < this.ctfoConfigBean.getDeleteThread(); i++) {
                CTFOAddCacheDeleteThread t = new CTFOAddCacheDeleteThread(i, this.ctfoConfigBean.getAddress(), tk, addr, latch);
                t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        CTFOAddCache.this.threadStatus = 1;
                        CTFOAddCache.this.msg = (t.getName() + " : " + e.getMessage());
                    }
                });
                t.start();
            }
            latch.await();
            if (this.threadStatus == 1) {
                DataCenterException dataCenterException = new DataCenterException(this.msg);
                throw dataCenterException;
            }
            this.c.delete(tk);
            logger.info("add cache[delete key " + addr + " deal finish]");
        }
    }

    private void modifyMonitorData()
        throws DataCenterException {
        String monitorkey = RedisUtil.getConfigKey("mon", "address");
        List<String> list = this.c.queryList(monitorkey, Long.valueOf(0L), Long.valueOf(-1L));
        for (int i = 0; i < this.ctfoConfigBean.getCtfoDataBean().size(); i++) {
            CTFODataBean ctfoDataBean = (CTFODataBean) this.ctfoConfigBean.getCtfoDataBean().get(i);
            this.c.addRList(monitorkey, new String[]{ctfoDataBean.getMaster_address()});
            this.c.addRList(monitorkey, new String[]{ctfoDataBean.getSlave_address()});
            if (!"-".equals(ctfoDataBean.getMaster_address())) {
                String masterkey = RedisUtil.getConfigKey("mon", ctfoDataBean.getMaster_address());
                this.c.addHash(masterkey, "addr", ctfoDataBean.getMaster_address());
                this.c.addHash(masterkey, "name", ctfoDataBean.getName());
                this.c.addHash(masterkey, "type", i + list.size() + "");
                this.c.addHash(masterkey, "status", "0");
                this.c.addHash(masterkey, "cpu", ctfoDataBean.getCpu());
                this.c.addHash(masterkey, "mem", ctfoDataBean.getMem());
                this.c.addHash(masterkey, "io", ctfoDataBean.getIo());
            }
            if (!"-".equals(ctfoDataBean.getSlave_address())) {
                String slavekey = RedisUtil.getConfigKey("mon", ctfoDataBean.getSlave_address());
                this.c.addHash(slavekey, "addr", ctfoDataBean.getSlave_address());
                this.c.addHash(slavekey, "name", ctfoDataBean.getName());
                this.c.addHash(slavekey, "type", i + list.size() + "");
                this.c.addHash(slavekey, "status", "1");
                this.c.addHash(slavekey, "cpu", ctfoDataBean.getCpu());
                this.c.addHash(slavekey, "mem", ctfoDataBean.getMem());
                this.c.addHash(slavekey, "io", ctfoDataBean.getIo());
            }
            StringBuffer sb = new StringBuffer();
            sb.append("add cache[modify monitor data :");
            sb.append(ctfoDataBean.getMaster_address());
            sb.append(",");
            sb.append(ctfoDataBean.getSlave_address());
            sb.append("]");
            logger.info(sb.toString());
        }
    }

    private void waitClient(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
