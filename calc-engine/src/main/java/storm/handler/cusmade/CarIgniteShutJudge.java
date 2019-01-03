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
    protected String initRedisKey() {
        return "vehCache.qy.ignite.shut.notice";
    }

    @Override
    protected boolean filter(final ImmutableMap<String, String> data) {
        String carStatus = data.get(DataKey._3201_CAR_STATUS);
        return StringUtils.isEmpty(carStatus);
    }

    @Override
    protected void prepareData(final @NotNull ImmutableMap<String, String> data) {
        final String vehicleId = data.get(DataKey.VEHICLE_ID);
        //缓存最后一帧soc
        String socStr = data.get(DataKey._7615_STATE_OF_CHARGE);
        if (StringUtils.isNotEmpty(socStr)) {
            double soc = stringToDouble(socStr);
            if (soc > 0) {
                lastSoc.put(vehicleId, soc);
            }
        }

        //缓存最后一帧里程
        String mileageStr = data.get(DataKey._2202_TOTAL_MILEAGE);
        if (StringUtils.isNotEmpty(mileageStr)) {
            double mileage = stringToDouble(mileageStr);
            if (mileage > 0) {
                lastMile.put(vehicleId, mileage);
            }
        }

        //缓存车辆最大车速
        if (MapUtils.isEmpty(readMemoryVehicleNotice(vehicleId))) {
            //没有触发通知，清空最大车速重新统计
            igniteShutMaxSpeed.remove(vehicleId);
        }
        double speed = stringToDouble(data.get(DataKey._2201_SPEED));
        if (igniteShutMaxSpeed.containsKey(vehicleId)) {
            double cacheSpeed = igniteShutMaxSpeed.get(vehicleId);
            if (speed > cacheSpeed) {
                igniteShutMaxSpeed.put(vehicleId, speed);
            }
        } else {
            igniteShutMaxSpeed.put(vehicleId, speed);
        }
    }

    /**
     * 字符串转double
     *
     * @param str
     * @return
     */
    private double stringToDouble(String str) {
        if (NumberUtils.isNumber(str)) {
            return Double.valueOf(str);
        }
        return 0;
    }

    @Override
    protected State initState(final ImmutableMap<String, String> data) {
        String carStatus = data.get(DataKey._3201_CAR_STATUS);
        switch (carStatus) {
            case "1":
                return State.BEGIN;
            case "2":
                return State.END;
            default:
                return State.UNKNOW;
        }
    }

    @Override
    protected @NotNull ImmutableMap<String, String> beginNoticeInit(@NotNull final ImmutableMap<String, String> data, final @NotNull String vehicleId, final @NotNull String platformReceiverTimeString) {
        LOG.debug("VID:{} 车辆点火首帧缓存初始化", vehicleId);
        String vin = data.get(DataKey.VEHICLE_NUMBER);
        return new ImmutableMap.Builder<String, String>()
            .put("msgType", NoticeType.IGNITE_SHUT_MESSAGE)
            .put("vid", vehicleId)
            .put("vin", vin)
            .put("msgId", UUID.randomUUID().toString())
            .build();
    }

    @Override
    protected Map<String, String> beginNoticeSend(final @NotNull ImmutableMap<String, String> data, final int count, final long timeout, @NotNull final String vehicleId) {
        final Map<String, String> socLowStartNotice = Maps.newHashMap(
            readMemoryVehicleNotice(vehicleId)
        );
        double soc = lastSoc.getOrDefault(vehicleId, 0d);
        String time = data.get(DataKey.TIME);
        socLowStartNotice.put("stime", time);
        socLowStartNotice.put("soc", soc + "");
        socLowStartNotice.put("ssoc", soc + "");
        socLowStartNotice.put("mileage", lastMile.getOrDefault(vehicleId, 0d) + "");
        socLowStartNotice.put(NOTICE_STATUS_KEY, NOTICE_START_STATUS);
        socLowStartNotice.put("location", buildLocation(data));
        socLowStartNotice.put("noticetime", createNoticeTime());
        LOG.debug("VID:{} 车辆点火通知发送 MSGID:{}", vehicleId, socLowStartNotice.get("msgId"));
        return socLowStartNotice;
    }

    @Override
    protected Map<String, String> endNoticeSend(final @NotNull ImmutableMap<String, String> data, final int count, final long timeout, @NotNull final String vehicleId) {
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

        double ssoc = stringToDouble(igniteShutEndNotice.get("ssoc"));
        double energy = Math.abs(ssoc - soc);
        igniteShutEndNotice.put("energy", energy + "");

        igniteShutEndNotice.put(NOTICE_STATUS_KEY, NOTICE_END_STATUS);
        igniteShutEndNotice.put("location", buildLocation(data));

        String time = data.get(DataKey.TIME);
        igniteShutEndNotice.put("etime", time);
        igniteShutEndNotice.put("noticetime", createNoticeTime());
        return igniteShutEndNotice;
    }
}
