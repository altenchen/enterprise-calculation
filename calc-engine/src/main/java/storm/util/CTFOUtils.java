package storm.util;

import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class CTFOUtils implements Serializable {

    private static final long serialVersionUID = 193000000001L;

    private static Logger LOG = LoggerFactory.getLogger(CTFOUtils.class);

    /**
     * RedisUtil.getConfigKey("address") -> "cfg-sys-address"
     * 1. 从控制库 cfg-sys-address 键下获取集群字典, 字典键为连接名, 字典值为主机地址和端口.
     * 2. 通过集群字典初始化 ShardedJedisPool
     */
    private static CTFODBManager ctfoDBManager;

    /**
     * RedisUtil.getConfigUserKey({dbName}) -> RedisUtil.getConfigUserKey("xny") -> "cfg-usr-xny"
     * 1. 计算表配置键
     * 2. 将尝试获取的逻辑表名作为 field 写入配置键, 值为0.
     */
    private static CTFOCacheDB ctfoCacheDB;

    /**
     * 1. getCTFOCacheKeys() 创建一次性使用的 CTFOCacheKeys
     * 2. queryHash(String key) 从集群查询结果
     * 2.1 RedisUtil.getKey({dbName}, {tableName}, {key}) -> RedisUtil.getKey("xyn", "realInfo", "vid") -> xyn-realInfo-{vid} => {tableKey}
     * 2.2 ShardControl.queryHash({tableKey}) -> ShardedJedis.hgetAll({tableKey})
     *
     * 3. 对数据的增删改查都是基于 ShardedJedisPool
     * 3.1 ConnectionPoolFactory.getShardedJedisPool 创建 ShardedJedisPool
     * 3.1.1 使用 Hash 算法为 MD5
     * 3.1.2 使用默认的键提取模式 Pattern.compile("\\{(.+?)\\}")
     */
    private static CTFOCacheTable ctfoCacheTable;

    /**
     * 1. 建立连接池
     * 2. RedisUtil.getDataPatternKey({dbName}, {tableName}) -> RedisUtil.getDataPatternKey("xyn", "realInfo") -> "xyn-realInfo-*"
     * 3. next() 每次调用从一个连接中查询一批键(默认一次最大10000)并替换暂存区, 如果取到0个则跳转到下一个连接
     * 3.1 调用 next() 返回 true 表示还有后续数据, 返回 false 表示集群全部查询完毕, 并且关闭所有连接, 方法不再可用.
     * 3.1 每次调用 next() 之后, 要立即调用 getKeys() 方法获取结果, 再次调用 next() 会丢失原来的结果
     */
    @SuppressWarnings("unused")
    @Deprecated
    private final CTFOCacheKeys ctfoCacheKeys = null;

    static {
        initCTFO();
    }

    private synchronized static void initCTFO() {

        boolean sucess = false;

        LOG.info("初始化 CTFO 开始.");

        try {
            LOG.info("初始化 CTFODBManager 开始.");

            // 192.168.1.185:6379 -> 0 -> cfg-sys-address -> [192.168.1.104:1001, 192.168.1.104:1002]
            final String address = ConfigUtils.getSysDefine().getCtfoCacheHost() + ":" + ConfigUtils.getSysDefine().getCtfoCachePort();
            LOG.info("CTFO address is tcp://{}", address);
            ctfoDBManager = DataCenter.newCTFOInstance("cache", address);

            if (null != ctfoDBManager) {
                LOG.info("初始化 CTFODBManager 成功.");

                try {
                    LOG.info("初始化 CTFOCacheDB 开始.");

                    final String dbNameKey = "ctfo.cacheDB";
                    // [192.168.1.104:1001, 192.168.1.104:1002] -> 0 -> xyn-*
                    final String dbName = ConfigUtils.getSysDefine().getCtfoCacheDB();
                    if(StringUtils.isEmpty(dbName)) {
                        LOG.error("配置[{}]不能为空", dbNameKey);
                        return;
                    }
                    if("cfg".equals(dbName)) {
                        LOG.warn("配置[{}]不能为cfg", dbNameKey);
                    }
                    if(dbName.indexOf("-") != -1) {
                        LOG.warn("配置[{}]不能为包含[-]", dbNameKey);
                    }
                    ctfoCacheDB = ctfoDBManager.openCacheDB(dbName);

                    if (null != ctfoCacheDB) {
                        LOG.info("初始化 CTFOCacheDB 成功.");

                        try {
                            LOG.info("初始化 CTFOCacheTable 开始.");

                            final String tableNameKey = "ctfo.cacheTable";
                            // [192.168.1.104:1001, 192.168.1.104:1002] -> 0 -> xyn-realInfo-*
                            final String tableName = ConfigUtils.getSysDefine().getCtfoCacheTable();
                            if(StringUtils.isEmpty(dbName)) {
                                LOG.error("配置[{}]不能为空", tableNameKey);
                                return;
                            }
                            if(dbName.indexOf("-") != -1) {
                                LOG.warn("配置[{}]不能为包含[-]", tableNameKey);
                            }
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
            LOG.info("初始化 CTFO 失败, 尝试重连.");
            reconnectionDefaultRedis();
        }
    }

    private static void reconnectionDefaultRedis() {
        int retryCount = 0;
        while (true) {
            try {
                retryCount++;
                if (null == ctfoCacheDB) {
                    try {
                        final String address = ConfigUtils.getSysDefine().getCtfoCacheHost() + ":" + ConfigUtils.getSysDefine().getCtfoCachePort();
                        ctfoDBManager = DataCenter.newCTFOInstance("cache", address);
                    } catch (Exception e) {
                        LOG.warn("初始化 CTFODBManager 异常.", e);
                    }

                    if (null != ctfoDBManager) {

                        final String dbName = ConfigUtils.getSysDefine().getCtfoCacheDB();
                        ctfoCacheDB = ctfoDBManager.openCacheDB(dbName);
                    }
                }

                if (null != ctfoCacheDB) {

                    final String tableName = ConfigUtils.getSysDefine().getCtfoCacheTable();
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
            reconnectionDefaultRedis();
        }

        return ctfoCacheTable;
    }

}
