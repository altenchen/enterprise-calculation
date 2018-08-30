package storm.util;

import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOCacheDB;
import com.ctfo.datacenter.cache.handle.CTFOCacheTable;
import com.ctfo.datacenter.cache.handle.CTFODBManager;
import com.ctfo.datacenter.cache.handle.DataCenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CTFOUtils implements Serializable {

    private static final long serialVersionUID = 193000000001L;

    private static Logger LOG = LoggerFactory.getLogger(CTFOUtils.class);

    private static final ConfigUtils CONFIG_UTILS = ConfigUtils.getInstance();

    private static CTFODBManager ctfoDBManager;
    private static CTFOCacheDB ctfoCacheDB;
    private static CTFOCacheTable ctfoCacheTable;

    static {
        initCTFO(CONFIG_UTILS.sysDefine);
    }

    private synchronized static void initCTFO(Properties conf) {

        boolean sucess = false;

        LOG.info("初始化 CTFO 开始.");

        try {
            LOG.info("初始化 CTFODBManager 开始.");

            // 192.168.1.185:6379 -> 0 -> cfg-sys-address -> [192.168.1.104:1001, 192.168.1.104:1002]
            final String address = conf.getProperty("ctfo.cacheHost") + ":" + conf.getProperty("ctfo.cachePort");
            LOG.info("CTFO address is tcp://{}", address);
            ctfoDBManager = DataCenter.newCTFOInstance("cache", address);

            if (null != ctfoDBManager) {
                LOG.info("初始化 CTFODBManager 成功.");

                try {
                    LOG.info("初始化 CTFOCacheDB 开始.");

                    // [192.168.1.104:1001, 192.168.1.104:1002] -> 0 -> xyn-*
                    final String dbName = conf.getProperty("ctfo.cacheDB");
                    ctfoCacheDB = ctfoDBManager.openCacheDB(dbName);

                    if (null != ctfoCacheDB) {
                        LOG.info("初始化 CTFOCacheDB 成功.");

                        try {
                            LOG.info("初始化 CTFOCacheTable 开始.");

                            // [192.168.1.104:1001, 192.168.1.104:1002] -> 0 -> xyn-realInfo-*
                            final String tableName = conf.getProperty("ctfo.cacheTable");
                            ctfoCacheTable = ctfoCacheDB.getTable(tableName);

                            if (null != ctfoCacheTable) {
                                LOG.info("初始化 CTFOCacheTable 成功, 开始加载数据表.");

                                LOG.info("初始化 CTFO 成功.");
                                sucess = true;
                            } else {
                                LOG.warn("初始化 CTFOCacheTable 失败.");
                            }
                        } catch (DataCenterException e) {
                            LOG.warn("初始化 CTFOCacheTable 异常", e);
                        }
                    } else {
                        LOG.warn("初始化 CTFOCacheDB 失败.");
                    }
                } catch (DataCenterException e) {
                    LOG.warn("初始化 CTFOCacheDB 异常", e);
                }
            } else {
                LOG.warn("初始化 CTFODBManager 失败.");
            }
        } catch (Exception e) {
            LOG.warn("初始化 CTFODBManager 异常", e);
        }

        if(!sucess) {
            LOG.info("初始化 CTFO 失败, 重连到默认Redis");
            reconnectionDefaultRedis(conf);
        }
    }

    private static void reconnectionDefaultRedis(Properties conf) {
        int retryCount = 0;
        while (true) {
            try {
                retryCount++;
                if (null == ctfoCacheDB) {
                    try {
                        final String address = conf.getProperty("ctfo.cacheHost") + ":" + conf.getProperty("ctfo.cachePort");
                        ctfoDBManager = DataCenter.newCTFOInstance("cache", address);
                    } catch (Exception e) {
                        LOG.warn("初始化 CTFODBManager 异常.", e);
                    }

                    if (null != ctfoDBManager) {

                        final String dbName = conf.getProperty("ctfo.cacheDB");
                        ctfoCacheDB = ctfoDBManager.openCacheDB(dbName);
                    }
                }

                if (null != ctfoCacheDB) {

                    final String tableName = conf.getProperty("ctfo.cacheTable");
                    ctfoCacheTable = ctfoCacheDB.getTable(tableName);
                }

                if(null != ctfoCacheTable) {

                    LOG.info("重连 CTFO 成功...");
                    break;
                }

                if (retryCount > 30) {
                    LOG.warn("重试超过30次");
                    try {
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException ie) {
                        LOG.warn("休眠异常", ie);
                    }
                }

                if (retryCount > 50) {
                    LOG.error("CTFO 重连超过50次");
                    LOG.error("CTFO 重连失败");
                    return;
                }

                LOG.info("正在进行 CTFO 重连....");

            } catch (DataCenterException e) {
                LOG.warn("CTFO 重连失败,3秒后继续重连....");

                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException ie) {
                    LOG.warn("休眠异常", ie);
                }
            }
        }
    }

    public static final CTFOCacheTable getDefaultCTFOCacheTable() {

        if (null == ctfoCacheTable) {
            reconnectionDefaultRedis(CONFIG_UTILS.sysDefine);
        }

        return ctfoCacheTable;
    }

}
