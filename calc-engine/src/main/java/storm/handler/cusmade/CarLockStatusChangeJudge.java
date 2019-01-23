package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.VehicleCache;
import storm.constant.FormatConstant;
import storm.constant.QualityOfService;
import storm.protocol.CommandType;
import storm.system.DataKey;
import storm.system.NoticeType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * 车辆锁止状态变化通知
 * @author 徐志鹏
 */
public final class CarLockStatusChangeJudge {

    private static final Logger logger = LoggerFactory.getLogger(CarLockStatusChangeJudge.class);

    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();

    /**
     * 车辆锁止状态变化通知
     * @param data 实时数据
     * @return 如果状态发生了变化，则返回状态变化通知.
     */
    @Nullable
    public Map<String, Object> carLockStatueChangeJudge(@NotNull final Map<String, String> data) {

        final Map<String, Object> result = new HashMap<>(16);

        if (MapUtils.isEmpty(data)) {
            return null;
        }

        final String msgType = data.get(DataKey.MESSAGE_TYPE);
        if(!CommandType.SUBMIT_REALTIME.equals(msgType)) {
            return null;
        }

        final String vid = data.get(DataKey.VEHICLE_ID);
        final String time = data.get(DataKey.TIME);
        if (StringUtils.isEmpty(vid) || StringUtils.isEmpty(time)) {
            return null;
        }

        final String lockFunction = data.get(DataKey._4710061_JILI_LOCK_FUNCTION);
        if(!NumberUtils.isDigits(lockFunction)) {
            return null;
        }
        if (!DataKey._4710061_JILI_LOCK_FUNCTION_DISABLE.equals(lockFunction)
                && !DataKey._4710061_JILI_LOCK_FUNCTION_ENABLE.equals(lockFunction)) {

            logger.warn("未定义的锁车功能状态值[{}]", lockFunction);
            return null;
        }

        final String lockStatus = data.get(DataKey._4710062_JILI_LOCK_STATUS);
        if(!NumberUtils.isDigits(lockStatus)) {
            return null;
        }
        if (!DataKey._4710062_JILI_LOCK_STATUS_UNLOCK.equals(lockStatus)
                && !DataKey._4710062_JILI_LOCK_STATUS_LOCKED.equals(lockStatus)) {

            logger.warn("未定义的锁车状态值[{}]", lockStatus);
            return null;
        }

        final ImmutableMap<String, String> jiliLockCache = VEHICLE_CACHE.getField(
            vid,
            VehicleCache.JILI_LOCK,
            () -> ImmutableMap.of());
        final String lockFunctionCache = jiliLockCache.get(DataKey._4710061_JILI_LOCK_FUNCTION);
        final String carLockStatusBefore = jiliLockCache.get(DataKey._4710062_JILI_LOCK_STATUS);

        if (StringUtils.equals(lockFunctionCache, lockFunction)
            && StringUtils.equals(carLockStatusBefore, lockStatus)) {
            return null;
        }

        try {
            VEHICLE_CACHE.putField(
                vid,
                VehicleCache.JILI_LOCK,
                new ImmutableMap.Builder<String, String>()
                    .put(DataKey._4710061_JILI_LOCK_FUNCTION, lockFunction)
                    .put(DataKey._4710062_JILI_LOCK_STATUS, lockStatus)
                    .build());
        } catch (ExecutionException e) {
            logger.error("吉利终端锁车状态写缓存异常", e);
        }

        final long currentTimeMillis = System.currentTimeMillis();
        final String noticeTime = DateFormatUtils.format(currentTimeMillis, FormatConstant.DATE_FORMAT);

        final Map<String, Object> notice = new HashMap<>(16);
        notice.put(QualityOfService.KEY, QualityOfService.AT_LEAST_ONCE);
        notice.put("msgType", NoticeType.VEH_LOCK_STATUS_CHANGE);
        notice.put("vid", vid);
        notice.put("noticetime", noticeTime);
        notice.put("lockFunctionStatusChange", lockFunction);
        if(DataKey._4710061_JILI_LOCK_FUNCTION_DISABLE.equals(lockFunction)) {
            notice.put("lockStatusChange", DataKey._4710062_JILI_LOCK_STATUS_DISABLE);
        } else {
            notice.put("lockStatusChange", lockStatus);
        }
        return notice;

    }
}

