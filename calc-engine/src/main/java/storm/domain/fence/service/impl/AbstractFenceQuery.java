package storm.domain.fence.service.impl;

import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dao.DataToRedis;
import storm.domain.fence.Fence;
import storm.domain.fence.service.IFenceQueryService;
import storm.domain.fence.status.FenceVehicleStatus;
import storm.util.ConfigUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 该类实现了以下基础的功能,子类只需要继承该类， 实现数据查询【dataQuery】这个接口即可, 比如从mysql, redis中获取数据等
 * 1. 判断是否存在电子围栏
 * 2. 判断电子围栏与规则是否关联
 * 3. 判断是否存在有效的电子围栏与车辆关联
 * 4. 数据检查,删除redis上的脏数据
 *
 * @author 智杰
 */
public abstract class AbstractFenceQuery implements IFenceQueryService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFenceQuery.class);

    /**
     * 上一次同步围栏规则时间
     */
    private long prevSyncTime = 0L;

    /**
     * 车辆与围栏规则
     * <vid, <fenceId, 围栏规则>>
     */
    protected Map<String, ImmutableMap<String, Fence>> vehicleFenceRuleMap = new HashMap<>(0);
    /**
     * 电子围栏与规则(事件)映射关系
     * <fenceId, [eventId, eventId, ...]>
     */
    protected Map<String, Set<String>> fenceEventMap = new HashMap<>(0);
    /**
     * 电子围栏与车辆映射关系
     * <fenceId, [vid, vid, ...]>
     */
    protected Map<String, Set<String>> fenceVehicleMap = new HashMap<>(0);

    /**
     * 刷新电子围栏缓存
     */
    private synchronized void refresh() {
        long flushTime = ConfigUtils.getSysDefine().getDbCacheFlushTime() * 1000;
        long currentTime = System.currentTimeMillis();
        if (vehicleFenceRuleMap == null || currentTime - this.prevSyncTime >= flushTime) {
            LOGGER.info("同步电子围栏规则");
            //到了刷新时间，重新同步规则
            dataQuery((vehicleFenceRuleMap, fenceEventMap, fenceVehicleMap) -> {
                this.vehicleFenceRuleMap = vehicleFenceRuleMap;
                this.fenceEventMap = fenceEventMap;
                this.fenceVehicleMap = fenceVehicleMap;
            });
            //更新最后一次同步时间
            this.prevSyncTime = System.currentTimeMillis();
            LOGGER.info("同步电子围栏规则结束");
        }
    }


    /**
     * 查询围栏列表
     *
     * @param vid 车辆ID
     * @return 车辆对应的围栏列表
     */
    @Override
    @NotNull
    public ImmutableMap<String, Fence> query(String vid) {
        refresh();
        ImmutableMap<String, Fence> result = vehicleFenceRuleMap.get(vid);
        return result == null ? ImmutableMap.of() : result;
    }

    @Override
    public boolean existFence(String fenceId) {
        return fenceEventMap.containsKey(fenceId);
    }

    @Override
    public boolean existFenceEvent(String fenceId, String eventId) {
        return fenceEventMap.containsKey(fenceId) && fenceEventMap.get(fenceId).contains(eventId);
    }

    @Override
    public boolean existFenceVehicle(String fenceId, String vehicleId) {
        return fenceVehicleMap.containsKey(fenceId) && fenceVehicleMap.get(fenceId).contains(vehicleId);
    }

    @Override
    public void dataCheck(final DataToRedis redis) {
        //数据检查之前先刷新一次缓存，得到最新的数据
        refresh();

        //判断围栏与车辆关系是否被解除
        Set<String> fenceVehicleStatusKeys = redis.hkeys(DataToRedis.REDIS_DB_6, FenceVehicleStatus.FENCE_VEHICLE_STATUS_CACHE);
        if (fenceVehicleStatusKeys != null) {
            //检查围栏车辆状态
            fenceVehicleStatusKeys.forEach(key -> {
                /**
                 * arr[0] 围栏ID
                 * arr[1] 车辆VID
                 */
                String[] arr = key.split("\\.");
                if (arr.length < 2) {
                    //key不正确，直接删除
                    redis.hdel(DataToRedis.REDIS_DB_6, FenceVehicleStatus.FENCE_VEHICLE_STATUS_CACHE, key);
                    return;
                }
                boolean exists = existFenceVehicle(arr[0], arr[1]);
                if (!exists) {
                    //车辆与围栏关系解除了，直接删除
                    redis.hdel(DataToRedis.REDIS_DB_6, FenceVehicleStatus.FENCE_VEHICLE_STATUS_CACHE, key);
                    return;
                }

            });
        }

        //判断围栏与事件关系是否被解除
        Set<String> fenceEventNoticeKeys = redis.hkeys(DataToRedis.REDIS_DB_6, FenceVehicleStatus.FENCE_EVENT_NOTICE_CACHE);
        if (fenceEventNoticeKeys != null) {
            fenceEventNoticeKeys.forEach(key -> {
                /**
                 * arr[0] 围栏ID
                 * arr[1] 事件ID
                 */
                String[] arr = key.split("\\.");
                if (arr.length < 2) {
                    //key不正确，直接删除
                    redis.hdel(DataToRedis.REDIS_DB_6, FenceVehicleStatus.FENCE_EVENT_NOTICE_CACHE, key);
                    return;
                }
                boolean exists = existFenceEvent(arr[0], arr[1]);
                if (!exists) {
                    //车辆与围栏关系解除了，直接删除
                    redis.hdel(DataToRedis.REDIS_DB_6, FenceVehicleStatus.FENCE_EVENT_NOTICE_CACHE, key);
                    return;
                }
            });
        }

    }

    /**
     * 数据查询逻辑
     *
     * @param callback 将结果通过回调初始化
     */
    protected abstract void dataQuery(DataInitCallback callback);

    protected interface DataInitCallback {
        /**
         * 初始化完成
         *
         * @param vehicleFenceRuleMap 车辆与围栏映射关系 <vid, <fenceId, 围栏规则>>
         * @param fenceEventMap       围栏与事件映射关系 <fenceId, [eventId, eventId, ...]>
         * @param fenceVehicleMap     围栏与车辆映射关系 <fenceId, [vid, vid, ...]>
         */
        void finishInit(Map<String, ImmutableMap<String, Fence>> vehicleFenceRuleMap, Map<String, Set<String>> fenceEventMap, Map<String, Set<String>> fenceVehicleMap);
    }

}
