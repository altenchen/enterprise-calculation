package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class CarIgniteShutJudge extends AbstractVehicleDelaySwitchJudge {

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

        igniteShutMaxSpeed.compute(
            vehicleId,
            (vid, cacheSpeed) -> {
                String speedString = data.get(DataKey._2201_SPEED);
                final double speed = NumberUtils.toDouble(speedString, 0d);
                if (null != cacheSpeed && cacheSpeed > speed) {
                    return  cacheSpeed;
                }
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
    protected State parseState(final ImmutableMap<String, String> data) {
        final String carStatus = data.get(DataKey._3201_CAR_STATUS);
        switch (carStatus) {
            case CAR_STATUS_IGNITE:
                return State.BEGIN;
            case CAR_STATUS_FLAMEOUT:
                return State.END;
            default:
                return State.UNKNOWN;
        }
    }

    @Override
    protected @NotNull ImmutableMap<String, String> initBeginNotice(@NotNull final ImmutableMap<String, String> data, final @NotNull String vehicleId, final @NotNull String platformReceiverTimeString) {
        LOG.debug("VID:{} 车辆点火首帧缓存初始化", vehicleId);
        String vin = data.get(DataKey.VEHICLE_NUMBER);
        if(getState(vehicleId) != State.BEGIN) {
            String speedString = data.get(DataKey._2201_SPEED);
            final double speed = NumberUtils.toDouble(speedString, 0d);
            igniteShutMaxSpeed.put(vehicleId, speed);
        }
        return new ImmutableMap.Builder<String, String>()
            .put("msgType", NoticeType.IGNITE_SHUT_MESSAGE)
            .put("vid", vehicleId)
            .put("vin", vin)
            .put("msgId", UUID.randomUUID().toString())
            .build();
    }

    @Override
    protected Map<String, String> buildBeginNotice(final @NotNull ImmutableMap<String, String> data, final int count, final long timeout, @NotNull final String vehicleId) {
        final Map<String, String> socLowStartNotice = Maps.newHashMap(
            readMemoryVehicleNotice(vehicleId)
        );
        final String socString = lastSoc.getOrDefault(vehicleId, 0d).toString();
        String time = data.get(DataKey.TIME);
        socLowStartNotice.put("stime", time);
        socLowStartNotice.put("soc", socString);
        socLowStartNotice.put("ssoc", socString);
        socLowStartNotice.put("mileage", lastMile.getOrDefault(vehicleId, 0d) + "");
        socLowStartNotice.put(NOTICE_STATUS_KEY, NOTICE_START_STATUS);
        socLowStartNotice.put("location", DataUtils.buildLocation(data));
        socLowStartNotice.put("noticetime", DataUtils.buildFormatTime());
        LOG.debug("VID:{} 车辆点火通知发送 MSGID:{}", vehicleId, socLowStartNotice.get("msgId"));
        return socLowStartNotice;
    }

    @Override
    protected Map<String, String> buildEndNotice(final @NotNull ImmutableMap<String, String> data, final int count, final long timeout, @NotNull final String vehicleId) {
        LOG.trace("VID:{} 车辆熄火通知发送", vehicleId);
        final ImmutableMap<String, String> igniteShutBeginNotice = readRedisVehicleNotice(vehicleId);
        if (MapUtils.isEmpty(igniteShutBeginNotice)) {
            return null;
        }
        final Map<String, String> igniteShutEndNotice = Maps.newHashMap(igniteShutBeginNotice);
        igniteShutEndNotice.putAll(readMemoryVehicleNotice(vehicleId));

        double soc = lastSoc.getOrDefault(vehicleId, 0d);
        igniteShutEndNotice.put("soc", soc + "");
        igniteShutEndNotice.put("mileage", lastMile.getOrDefault(vehicleId, 0d) + "");
        igniteShutEndNotice.put("maxSpeed", igniteShutMaxSpeed.getOrDefault(vehicleId, 0d) + "");

        double ssoc = NumberUtils.toDouble(igniteShutEndNotice.get("ssoc"));
        double energy = Math.abs(ssoc - soc);
        igniteShutEndNotice.put("energy", energy + "");

        igniteShutEndNotice.put(NOTICE_STATUS_KEY, NOTICE_END_STATUS);
        igniteShutEndNotice.put("location", DataUtils.buildLocation(data));

        String time = data.get(DataKey.TIME);
        igniteShutEndNotice.put("etime", time);
        igniteShutEndNotice.put("noticetime", DataUtils.buildFormatTime());
        return igniteShutEndNotice;
    }
}
