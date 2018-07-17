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

    private static Logger logger = LoggerFactory.getLogger(CTFOUtils.class);

    private static final ConfigUtils configUtils = ConfigUtils.getInstance();

    private static final Map<String, CTFOCacheDB> dbMap = new TreeMap<>();
    private static final Map<String, CTFOCacheTable> tableMap = new TreeMap<>();

    private static CTFODBManager ctfoDBManager;
    private static CTFOCacheDB ctfoCacheDB;
    private static CTFOCacheTable ctfoCacheTable;

    static {
        initCTFO(configUtils.sysDefine);
    }

    private synchronized static void initCTFO(Properties conf) {
        logger.info("初始化 CTFO 开始.");

        try {
            logger.info("初始化 CTFODBManager 开始.");

            final String address = conf.getProperty("ctfo.cacheHost") + ":" + conf.getProperty("ctfo.cachePort");
            logger.info("CTFO address is tcp://{}", address);
            ctfoDBManager = DataCenter.newCTFOInstance("cache", address);

            if (null != ctfoDBManager) {
                logger.info("初始化 CTFODBManager 成功.");

                try {
                    logger.info("初始化 CTFOCacheDB 开始.");

                    ctfoCacheDB = ctfoDBManager.openCacheDB(conf.getProperty("ctfo.cacheDB"));

                    if (null != ctfoCacheDB) {
                        logger.info("初始化 CTFOCacheDB 成功.");

                        try {
                            logger.info("初始化 CTFOCacheTable 开始.");

                            ctfoCacheTable = ctfoCacheDB.getTable(conf.getProperty("ctfo.cacheTable"));
                            if (null != ctfoCacheTable) {
                                logger.info("初始化 CTFOCacheTable 成功, 开始加载数据表.");

                                initDBTables();
                                logger.info("初始化 CTFO 成功.");
                                return;
                            } else {
                                logger.warn("初始化 CTFOCacheTable 失败.");
                            }
                        } catch (DataCenterException e) {
                            logger.warn("初始化 CTFOCacheTable 异常", e);
                        }
                    } else {
                        logger.warn("初始化 CTFOCacheDB 失败.");
                    }
                } catch (DataCenterException e) {
                    logger.warn("初始化 CTFOCacheDB 异常", e);
                }
            } else {
                logger.warn("初始化 CTFODBManager 失败.");
            }
        } catch (Exception e) {
            logger.warn("初始化 CTFODBManager 异常", e);
        }

        logger.info("初始化 CTFO 失败, 重连到默认Redis");
        reconnectionDefaultRedis(conf);
    }

    private static void reconnectionDefaultRedis(Properties conf) {
        int retry = 0;
        while (true) {
            try {
                retry++;
                if (null != ctfoCacheDB) {
                    ctfoCacheTable = ctfoCacheDB.getTable(conf.getProperty("ctfo.cacheTable"));
                } else {
                    try {
                        String addr = conf.getProperty("ctfo.cacheHost") + ":" + conf.getProperty("ctfo.cachePort");
                        ctfoDBManager = DataCenter.newCTFOInstance("cache", addr);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println(e);
                    }
                    if (null != ctfoDBManager) {

                        ctfoCacheDB = ctfoDBManager.openCacheDB(conf.getProperty("ctfo.cacheDB"));
                        ctfoCacheTable = ctfoCacheDB.getTable(conf.getProperty("ctfo.cacheTable"));
                        System.out.println("------ CTFOUtils relink success...");
                        logger.info("重连 CTFO 成功...");
                    }
                    if (null != ctfoCacheDB) {
                        ctfoCacheTable = ctfoCacheDB.getTable(conf.getProperty("ctfo.cacheTable"));
                    }

                }
                if (ctfoCacheTable != null) {
                    logger.warn("----------redis重连成功！");
                    break;
                }
                if (30 < retry) {
                    try {
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
                if (50 < retry) {
                    System.out.println("------ CTFOUtils relink fault...");
                    logger.warn("----------Ctfo redis重连失败！");
                    break;
                }
                logger.info("----------正在进行redis重连....");
            } catch (DataCenterException e) {
                logger.warn("----------redis重连失败,3s后继续重连....");
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public static final CTFOCacheTable getDefaultCTFOCacheTable() {
        try {
            if (null == ctfoCacheTable) {
                reconnectionDefaultRedis(configUtils.sysDefine);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ctfoCacheTable;
    }

    private static void initDBTables() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        int year = calendar.get(Calendar.YEAR);
        String table = configUtils.sysDefine.getProperty("ctfo.supplyTable");
        for (int y = year; y > year - 3; y--) {
            try {
                initDBTable(ctfoDBManager, y + "", table);
            } catch (Exception e) {
                logger.warn("", e);
            }
        }
    }

    private static void initDBTable(CTFODBManager manager, String db, String table)
        throws DataCenterException {

        CTFOCacheDB cacheDB = manager.openCacheDB(db);
        CTFOCacheTable cacheTable = cacheDB.getTable(table);
        dbMap.put(db, cacheDB);
        tableMap.put(db, cacheTable);
    }

    private static CTFOCacheDB getDB(String name) {
        CTFOCacheDB cacheDB = dbMap.get(name);
        try {
            if (null == cacheDB) {
                cacheDB = ctfoDBManager.openCacheDB(name);
                dbMap.put(name, cacheDB);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("----------redis重连失败,3s后继续重连....");
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
        return cacheDB;
    }

    private static void reconnection(Properties conf, String name) {
        while (true) {
            try {
                CTFOCacheDB cacheDB = getDB(name);
                CTFOCacheTable cacheTable = cacheDB.getTable(conf.getProperty("ctfo.supplyTable"));
                if (cacheTable != null) {
                    logger.warn("----------supply redis重连成功！");
                    tableMap.put(name, cacheTable);
                    break;
                }
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                logger.info("----------正在进行supply redis重连....");
            } catch (Exception e) {
                logger.warn("----------redis重连失败,3s后继续重连....");
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public static final CTFOCacheTable getCacheTable(String name) {
        CTFOCacheTable cacheTable = null;
        try {
            cacheTable = tableMap.get(name);
            if (null == cacheTable) {
                reconnection(configUtils.sysDefine, name);
                cacheTable = tableMap.get(name);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return cacheTable;
    }
}
