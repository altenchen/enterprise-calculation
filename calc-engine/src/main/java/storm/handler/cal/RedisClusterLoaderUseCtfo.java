package storm.handler.cal;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOCacheKeys;
import com.ctfo.datacenter.cache.handle.CTFOCacheTable;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dao.DataToRedis;
import storm.util.CTFOUtils;

/**
 * 从 Redis 集群加载数据并存储到本地缓存
 * TODO: 合并到 SysRealDataCache 类中, 统一处理.
 * @author xzp
 */
public class RedisClusterLoaderUseCtfo {

    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterLoaderUseCtfo.class);

    // region LastData

    /**
     * 车辆最后一帧数据
     */
    private static final Cache<String, Map<String, String>> LAST_DATA_CACHE = CacheBuilder.newBuilder()
        // 数据在举例最后一次访问十分钟后过期
        .expireAfterAccess(10, TimeUnit.MINUTES)
        // 最大缓存1500万条记录
        .maximumSize(15000000)
        .build();

    /**
     * 被缓存车辆最后一帧数据的车辆Id队列
     */
    public static LinkedBlockingQueue<String> cachedVehicleIdQueue = new LinkedBlockingQueue<>(20000000);


    /**
     * 数据加载器
     */
    private static class ClusterKeysLoader implements Runnable, Serializable {

        private static final long serialVersionUID = 112345600014L;

        private static Logger LOG = LoggerFactory.getLogger(ClusterKeysLoader.class);

        @Nullable
        private final ImmutableCollection<String> clusterKeys;

        private boolean complete = false;

        public ClusterKeysLoader(
            @Nullable final ImmutableCollection<String> clusterKeys) {

            this.clusterKeys = clusterKeys;
        }

        @Override
        public void run() {

            if (CollectionUtils.isNotEmpty(clusterKeys)) {
                clusterKeys.forEach(this::loadByClusterKey);
            }

            complete = true;
        }

        /**
         * 从 redis 集群中获取数据用于统计
         *
         * @param clusterKey
         */
        void loadByClusterKey(
            @NotNull final String clusterKey) {

                if (StringUtils.isEmpty(clusterKey)) {
                    return;
                }

                final int clusterKeyPartCount = 3;

                // dbName_tableName_key
                final String[] splits = clusterKey.split("-", clusterKeyPartCount);
                if(splits.length < clusterKeyPartCount) {
                    return;
                }

                final String vid = splits[2];
                if (StringUtils.isEmpty(vid)) {
                    return;
                }

                final Map<String, String> fatMap;
                try {
                    fatMap = CTFOUtils.getDefaultCTFOCacheTable().queryHash(vid);
                } catch (DataCenterException e) {
                    LOG.warn("从 redis 集群加载数据异常.", e);
                    return;
                }
                if (MapUtils.isEmpty(fatMap)) {
                    return;
                }

                final Map<String, String> thinMap = Maps.newHashMapWithExpectedSize(fatMap.size());
                //不缓存无用的数据项，减小缓存大小
                for (final Map.Entry<String, String> entry : fatMap.entrySet()) {
                    final String key = entry.getKey();
                    final String value = entry.getValue();
                    if (StringUtils.isNotBlank(key) && StringUtils.isNotEmpty(value)
                        // useful_dataKey, 不是有效数据
                        && !key.startsWith("useful")
                        // newest_dataKey, 不是最新数据
                        && !key.startsWith("newest")
                        // 以下 dataKey 不加载
                        && !"2001".equals(key)
                        && !"2002".equals(key)
                        && !"2003".equals(key)
                        && !"2101".equals(key)
                        && !"2103".equals(key)
                        && !"7001".equals(key)
                        && !"7003".equals(key)
                        && !"7101".equals(key)
                        && !"7103".equals(key)) {
                        thinMap.put(key, value);
                    }
                }

                // 多个线程同时入队
                cachedVehicleIdQueue.offer(vid);
                // 多个线程同时进行断面缓存
                LAST_DATA_CACHE.put(vid, thinMap);
        }

        public boolean isComplete() {
            return complete;
        }
    }

    private synchronized static boolean loadLastDataFromRedisCluster() {

        try {
            final CTFOCacheTable ctfoCacheTable = CTFOUtils.getDefaultCTFOCacheTable();
            final CTFOCacheKeys ctfoCacheKeys = ctfoCacheTable.getCTFOCacheKeys();

            final int processors = Runtime.getRuntime().availableProcessors();
            final ExecutorService fixedThreadPool = Executors.newFixedThreadPool(processors);

            final List<ClusterKeysLoader> clusterKeysLoaders = Lists.newLinkedList();

            // 查找所有集群 key, 并通过线程池来加载值.

            while (ctfoCacheKeys.next()) {

                // 这一批键是在同一个 redis 节点上的
                final List<String> clusterKeys = ctfoCacheKeys.getKeys();
                if(CollectionUtils.isNotEmpty(clusterKeys)) {

                    final ClusterKeysLoader clusterKeysLoader =
                        new ClusterKeysLoader(ImmutableList.copyOf(clusterKeys));
                    clusterKeysLoaders.add(clusterKeysLoader);
                    fixedThreadPool.execute(clusterKeysLoader);
                }
            }

            // 等待所有线程执行完毕

            while (!clusterKeysLoaders.isEmpty()) {

                TimeUnit.MILLISECONDS.sleep(100);

                for (int index = clusterKeysLoaders.size() - 1; index >= 0; index--) {

                    final ClusterKeysLoader clusterKeysLoader = clusterKeysLoaders.get(index);

                    if (clusterKeysLoader.isComplete()) {
                        clusterKeysLoaders.remove(index);
                    }
                }

            };

            return true;

        } catch (Exception e) {
            LOG.warn("Redis 集群初始化实时数据计算异常", e);
        }
        return false;
    }

    /**
     * 标记并确保只初始化一次 LastRecord
     */
    private static boolean lastDataIsLoaded = false;

    private synchronized static void initLastDataFromRedisCluster() {

        lastDataIsLoaded = loadLastDataFromRedisCluster();
    }

    /**
     * 重启的时候获取集群中车辆最后一条数据, 数据只会初始化一次. 但是 SysRealDataCache 类会更新这个缓存.
     *
     * @return
     */
    @NotNull
    public synchronized static Cache<String, Map<String, String>> getLastDataCache() {
        try {
            if (!lastDataIsLoaded) {
                initLastDataFromRedisCluster();
            }
        } catch (Exception e) {
            LOG.warn("从 Redis 集群初始化车辆最后一帧数据异常.", e);
        }
        return LAST_DATA_CACHE;
    }

    // endregion LastData

    // region CarInfo

    /**
     * <VIN, [vid, 终端ID, 车牌号, 使用单位, 存放地点, 汽车厂商, 终端厂商, 终端类型, 车辆类型, 行政区域, 车辆类别, 联系人, 出厂时间, 注册时间, 是否注册]>
     */
    private static final Cache<String, String[]> CAR_INFO_CACHE = CacheBuilder.newBuilder()
        // 数据在举例最后一次访问一小时后过期
        .expireAfterAccess(60, TimeUnit.MINUTES)
        // 最大缓存1500万条记录
        .maximumSize(15000000)
        .build();

    private synchronized static boolean loadCarInfoCacheFromRedis() {

        try {

            final DataToRedis redis = new DataToRedis();

            // 车辆鉴权信息缓存
            final int dbIndex = 0;

            // 车辆鉴权信息
            final String redisKey = "XNY.CARINFO";

            // 固定的数据长度
            final int fixedValueCount = 15;

            // FIELD [VIN]
            // VALUE [vid, 终端ID, 车牌号, 使用单位, 存放地点, 汽车厂商, 终端厂商, 终端类型, 车辆类型, 行政区域, 车辆类别, 联系人, 出厂时间, 注册时间, 是否注册]
            final Map<String, String> map = redis.hashGetAllMapByKeyAndDb(redisKey, dbIndex);

            if(MapUtils.isEmpty(map)) {
                return true;
            }

            for (final Map.Entry<String, String> entry : map.entrySet()) {

                final String key = entry.getKey();
                final String value = entry.getValue();

                if (StringUtils.isBlank(key)
                    || StringUtils.isEmpty(value)) {
                    continue;
                }

                final String[] info = value.split(",");

                if (info.length != fixedValueCount) {
                    continue;
                }

                CAR_INFO_CACHE.put(key, info);
            }

        } catch (Exception e) {
            LOG.warn("从 Redis 加载车辆信息异常", e);
            return false;
        }

        return true;
    }

    /**
     * 标记并确保只初始化一次 CarInfo
     */
    private static boolean carInfoIsLoaded = false;

    private synchronized static void initCarInfoCacheFromRedis() {
        carInfoIsLoaded = loadCarInfoCacheFromRedis();
    }

    /**
     * 从 Redis 初始化车辆信息, 数据只会初始化一次. 但是 SysRealDataCache 类会更新这个缓存.
     * @return
     */
    @NotNull
    public synchronized static Cache<String, String[]> getCarInfoCache() {
        try {
            if (!carInfoIsLoaded) {
                initCarInfoCacheFromRedis();
            }
        } catch (Exception e) {
            LOG.warn("从 Redis 初始化车辆信息异常.", e);
        }
        return CAR_INFO_CACHE;
    }

    // endregion CarInfo

}
