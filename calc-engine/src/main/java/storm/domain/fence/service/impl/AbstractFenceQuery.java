package storm.domain.fence.service.impl;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dao.DataToRedis;
import storm.domain.fence.Fence;
import storm.domain.fence.service.IFenceQueryService;
import storm.domain.fence.status.FenceVehicleStatus;
import storm.util.ConfigUtils;
import storm.util.DataUtils;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

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
    private Map<String, ImmutableMap<String, Fence>> vehicleFenceRuleMap = new HashMap<>(0);
    /**
     * 电子围栏与规则(事件)映射关系
     * <fenceId, [eventId, eventId, ...]>
     */
    private Map<String, Set<String>> fenceEventMap = new HashMap<>(0);
    /**
     * 电子围栏缓存
     * <fenceId, Fence>
     */
    private Map<String, Fence> fenceMap = new HashMap<>(0);
    /**
     * 电子围栏与车辆映射关系
     * <fenceId, [vid, vid, ...]>
     */
    private Map<String, Set<String>> fenceVehicleMap = new HashMap<>(0);

    /**
     * 围栏与车辆状态删除标志
     */
    private Map<String, Set<String>> deleteFenceVehicleStatusFlag = new HashMap<>();

    private DataToRedis redis;

    private static final Lock LOCK = new ReentrantLock();

    /**
     * 是否检查中
     * true 在执行数据检查
     */
    private static boolean check = false;

    AbstractFenceQuery(final DataToRedis redis) {
        this.redis = redis;
    }

    /**
     * 刷新电子围栏缓存
     */
    private synchronized void refresh() {
        try {
            long flushTime = ConfigUtils.getSysDefine().getDbCacheFlushtime() * 1000;
            long currentTime = System.currentTimeMillis();
            if (vehicleFenceRuleMap == null || currentTime - this.prevSyncTime >= flushTime) {
                LOGGER.info("同步电子围栏规则");
                //到了刷新时间，重新同步规则
                dataQuery((vehicleFenceRuleMap, fenceEventMap, fenceVehicleMap, fenceMap) -> {
                    this.vehicleFenceRuleMap = vehicleFenceRuleMap;
                    this.fenceEventMap = fenceEventMap;
                    this.fenceVehicleMap = fenceVehicleMap;
                    this.fenceMap = fenceMap;
                    this.deleteFenceVehicleStatusFlag.clear();
                });
                LOGGER.info("同步电子围栏规则结束");
                //更新最后一次同步时间
                this.prevSyncTime = System.currentTimeMillis();
            }
        } catch (Exception e) {
            LOGGER.error("电子围栏同步数据异常", e);
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
    public boolean existFence(String fenceId, long time) {
        return Optional
            .ofNullable(fenceMap.get(fenceId))
            .map(fence -> fence.active(time))
            .orElse(false);
    }

    @Override
    public boolean existFenceEvent(String fenceId, String eventId) {
        return fenceEventMap.containsKey(fenceId) && fenceEventMap.get(fenceId).contains(eventId);
    }

    @Override
    public boolean existFenceVehicle(String fenceId, String vehicleId) {
        return fenceVehicleMap.containsKey(fenceId) && fenceVehicleMap.get(fenceId).contains(vehicleId);
    }

    /**
     * 数据检查
     * 1、检查围栏与车辆关系是否解除
     * 2、检查围栏与事件关系是否解除
     * 注意：开多个executor会重复发送结束通知
     */
    @Override
    public void dataCheck(BiConsumer<String, String> noticeCallback) {
        LOCK.lock();
        if( check ){
            LOCK.unlock();
            return;
        }
        check = true;
        LOCK.unlock();

        try {
            long time = System.currentTimeMillis();
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
                    boolean existsFence = existFence(arr[0], time);
                    if( !existsFence ){
                        //围栏处理于激活时间外
                        redis.hdel(DataToRedis.REDIS_DB_6, FenceVehicleStatus.FENCE_VEHICLE_STATUS_CACHE, key);
                        return;
                    }
                    boolean exists = existFenceVehicle(arr[0], arr[1]);
                    if (!exists) {
                        //车辆与围栏关系解除了，直接删除
                        redis.hdel(DataToRedis.REDIS_DB_6, FenceVehicleStatus.FENCE_VEHICLE_STATUS_CACHE, key);
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
                    boolean existsFence = existFence(arr[0], time);
                    if( !existsFence ){
                        //围栏处理于激活时间外
                        filterRedisBeginEventNotice(key, noticeCallback);
                        return;
                    }
                    boolean existEvent = existFenceEvent(arr[0], arr[1]);
                    if (!existEvent) {
                        //车辆与围栏关系解除了，直接删除
                        filterRedisBeginEventNotice(key, noticeCallback);
                    }
                });
            }
        } catch (Exception e) {
            LOGGER.error("数据检查出现异常", e);
        }
        check = false;
    }

    /**
     * 将redis中 eventStage = BEGIN的过滤出来
     * @param redisKey
     */
    private void filterRedisBeginEventNotice(String redisKey, BiConsumer<String, String> noticeCallback){
        try {
            String noticeString = redis.mapGet(DataToRedis.REDIS_DB_6, FenceVehicleStatus.FENCE_EVENT_NOTICE_CACHE, redisKey);
            String outside = "OUTSIDE";
            String inside = "INSIDE";
            Map noticeMap = JSON.parseObject(noticeString, Map.class);
            String vid = noticeMap.get("vehicleId").toString();
            if( !"BEGIN".equals(noticeMap.get("eventStage")) ){
                return;
            }
            String fromArea = noticeMap.get("fromArea").toString();
            if( fromArea.equals(outside) ){
                noticeMap.put("fromArea", inside);
                noticeMap.put("gotoArea", outside);
            }else{
                noticeMap.put("fromArea", outside);
                noticeMap.put("gotoArea", inside);
            }
            noticeMap.put("eventStage", "END");
            noticeMap.put("noticeTime", DataUtils.buildFormatTime(
                System.currentTimeMillis()
            ));
            noticeCallback.accept(vid, JSON.toJSONString(noticeMap));
            redis.hdel(DataToRedis.REDIS_DB_6, FenceVehicleStatus.FENCE_EVENT_NOTICE_CACHE, redisKey);
        } catch (Exception e) {
            LOGGER.error("JSON反序列化 " + FenceVehicleStatus.FENCE_EVENT_NOTICE_CACHE + " 出现异常, key : " + redisKey, e);
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
         * @param fenceMap            围栏缓存 <fenceId, Fence>
         */
        void finishInit(Map<String, ImmutableMap<String, Fence>> vehicleFenceRuleMap, Map<String, Set<String>> fenceEventMap, Map<String, Set<String>> fenceVehicleMap, Map<String, Fence> fenceMap);
    }

    @Override
    public void deleteFenceVehicleStatusCache(final String fenceId, final String vid, final Set<String> eventIds) {
        if (deleteFenceVehicleStatusFlag.containsKey(fenceId) && deleteFenceVehicleStatusFlag.get(fenceId).contains(vid)) {
            //该缓存已从redis删除
            return;
        }

        //删除围栏车辆状态
        String deleteFenceVehicleRedisKey = fenceId + "." + vid;
        redis.hdel(DataToRedis.REDIS_DB_6, FenceVehicleStatus.FENCE_VEHICLE_STATUS_CACHE, deleteFenceVehicleRedisKey);

        //删除车辆缓存通知
        if( eventIds != null ){
            redis.hdel(DataToRedis.REDIS_DB_6, FenceVehicleStatus.FENCE_EVENT_NOTICE_CACHE, eventIds.toArray(new String[0]));
        }

        Set<String> vids = deleteFenceVehicleStatusFlag.getOrDefault(fenceId, new HashSet<>());
        vids.add(vid);
        deleteFenceVehicleStatusFlag.put(fenceId, vids);
    }

}
