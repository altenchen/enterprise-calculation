package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dto.notice.IgniteShutNotice;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.util.ConfigUtils;
import storm.util.DataUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 车辆点火熄火通知
 *
 * @author 智杰
 */
public class CarIgniteShutJudge extends AbstractVehicleDelaySwitchJudge<IgniteShutNotice> {

    private static final Logger LOG = LoggerFactory.getLogger(CarIgniteShutJudge.class);

    /**
     * 车辆点火至熄火这段期间最大车速
     * <vid, speed>
     */
    private Map<String, Double> igniteShutMaxSpeed = new HashMap<>();

    /**
     * 最后一帧soc
     * <vid, soc>
     */
    private Map<String, Double> lastSoc = new HashMap<>();

    /**
     * 最后一帧里程
     * <vid, mile>
     */
    private Map<String, Double> lastMile = new HashMap<>();

    /**
     * 车辆点火熄火最大车速redis key
     */
    private static final String MAX_SPEED_REDIS_KEY = "vehCache.qy.ignite.shut.max.speed";

    public CarIgniteShutJudge() {
        super(ConfigUtils.getSysDefine().getNoticeIgniteTriggerContinueCount(),
            ConfigUtils.getSysDefine().getNoticeIgniteTriggerTimeoutMillisecond(),
            ConfigUtils.getSysDefine().getNoticeShutTriggerContinueCount(),
            ConfigUtils.getSysDefine().getNoticeShutTriggerTimeoutMillisecond());
    }

    @Override
    protected String buildRedisKey() {
        return "vehCache.qy.ignite.shut.notice";
    }

    @Override
    protected boolean ignore(final ImmutableMap<String, String> data) {
        String carStatus = data.get(DataKey._3201_CAR_STATUS);
        return StringUtils.isEmpty(carStatus);
    }

    @Override
    protected void beforeProcess(@NotNull final ImmutableMap<String, String> data) {
        final String vehicleId = data.get(DataKey.VEHICLE_ID);

        // 缓存最后一帧soc
        cacheLastUsefulSoc(vehicleId, data);

        // 缓存最后一帧里程
        cacheLastUsefulTotalMileage(vehicleId, data);

        // 缓存车辆最大车速
        cacheMaxSpeed(vehicleId, data);
    }

    private void cacheMaxSpeed(
        @NotNull final String vehicleId,
        final @NotNull ImmutableMap<String, String> data) {

        //如果当前内存中没有最大车速记录，则从redis恢复
        if( !igniteShutMaxSpeed.containsKey(vehicleId) ){
            String redisSpeed = readRedisCache(MAX_SPEED_REDIS_KEY, vehicleId);
            igniteShutMaxSpeed.put(vehicleId, NumberUtils.toDouble(redisSpeed, 0d));
        }

        igniteShutMaxSpeed.compute(
            vehicleId,
            (vid, cacheSpeed) -> {
                String speedString = data.get(DataKey._2201_SPEED);
                final double speed = NumberUtils.toDouble(speedString, 0d);
                if (null != cacheSpeed && cacheSpeed > speed) {
                    return cacheSpeed;
                }
                //将最大车速写入redis
                writeRedisCache(MAX_SPEED_REDIS_KEY, vehicleId, speed + "");
                return speed;
            }
        );
    }

    private void cacheLastUsefulTotalMileage(final String vehicleId, final @NotNull ImmutableMap<String, String> data) {
        String mileageStr = data.get(DataKey._2202_TOTAL_MILEAGE);
        if (StringUtils.isNotEmpty(mileageStr)) {
            double mileage = NumberUtils.toDouble(mileageStr);
            if (mileage > 0) {
                lastMile.put(vehicleId, mileage);
            }
        }
    }

    private void cacheLastUsefulSoc(final String vehicleId, final @NotNull ImmutableMap<String, String> data) {
        String socStr = data.get(DataKey._7615_STATE_OF_CHARGE);
        if (StringUtils.isNotEmpty(socStr)) {
            double soc = NumberUtils.toDouble(socStr);
            if (soc > 0) {
                lastSoc.put(vehicleId, soc);
            }
        }
    }

    private static final String CAR_STATUS_IGNITE = "1";
    private static final String CAR_STATUS_FLAMEOUT = "2";

    @Override
    protected NoticeState parseState(final ImmutableMap<String, String> data) {
        final String carStatus = data.get(DataKey._3201_CAR_STATUS);
        switch (carStatus) {
            case CAR_STATUS_IGNITE:
                return NoticeState.BEGIN;
            case CAR_STATUS_FLAMEOUT:
                return NoticeState.END;
            default:
                return NoticeState.UNKNOWN;
        }
    }

    @NotNull
    @Override
    protected IgniteShutNotice initBeginNotice(
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString) {

        LOG.debug("VID:{} 车辆点火首帧缓存初始化", vehicleId);
        String vin = data.get(DataKey.VEHICLE_NUMBER);
        if (getState(vehicleId) != NoticeState.BEGIN) {
            String speedString = data.get(DataKey._2201_SPEED);
            final double speed = NumberUtils.toDouble(speedString, 0d);
            igniteShutMaxSpeed.put(vehicleId, speed);
            //将车速写入redis
            writeRedisCache(MAX_SPEED_REDIS_KEY, vehicleId, speed + "");
        }

        IgniteShutNotice notice = new IgniteShutNotice();
        notice.setVid(vehicleId);
        notice.setVin(vin);
        notice.setMsgId(UUID.randomUUID().toString());
        notice.setMsgType(NoticeType.IGNITE_SHUT_MESSAGE);

        return notice;
    }

    @Override
    protected void buildBeginNotice(
        @NotNull final ImmutableMap<String, String> data,
        final int count,
        final long timeout,
        @NotNull final String vehicleId,
        @NotNull final IgniteShutNotice notice) {

        LOG.debug("VID:{} 车辆点火通知发送 MSGID:{}", vehicleId, notice.getMsgId());
        final String socString = lastSoc.getOrDefault(vehicleId, 0d).toString();
        final String noticeTime = DataUtils.buildFormatTime();

        notice.setStime(data.get(DataKey.TIME));
        notice.setSoc(socString);
        notice.setSsoc(socString);
        notice.setMileage(lastMile.getOrDefault(vehicleId, 0d) + "");
        notice.setLocation(DataUtils.buildLocation(data));
        //兼容以前的noticetime， 后期删除掉
        notice.setNoticetime(noticeTime);
        notice.setNoticeTime(noticeTime);
    }

    @Override
    protected void buildEndNotice(
        @NotNull final ImmutableMap<String, String> data,
        final int count,
        final long timeout,
        @NotNull final String vehicleId,
        @NotNull final IgniteShutNotice notice) {

        LOG.trace("VID:{} 车辆熄火通知发送", vehicleId);
        double soc = lastSoc.getOrDefault(vehicleId, 0d);
        final String noticeTime = DataUtils.buildFormatTime();

        notice.setSoc(soc + "");
        notice.setMileage(lastMile.getOrDefault(vehicleId, 0d).toString());
        notice.setMaxSpeed(igniteShutMaxSpeed.getOrDefault(vehicleId, 0d).toString());

        double ssoc = NumberUtils.toDouble(notice.getSsoc());
        double energy = Math.abs(ssoc - soc);
        notice.setEnergy(energy + "");

        notice.setLocation(DataUtils.buildLocation(data));
        notice.setEtime(data.get(DataKey.TIME));
        //兼容以前的noticetime， 后期删除掉
        notice.setNoticetime(noticeTime);
        notice.setNoticeTime(noticeTime);
    }

}
