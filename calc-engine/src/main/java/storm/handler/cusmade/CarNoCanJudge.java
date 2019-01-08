package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.VehicleCache;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.util.ConfigUtils;
import storm.util.DataUtils;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * @author: xzp, xzj
 * @date: 2018-06-14
 * @date 2019-1-2
 * @description: 无CAN车辆审计者
 */
public final class CarNoCanJudge extends AbstractVehicleDelaySwitchJudge {
    private static final Logger LOG = LoggerFactory.getLogger(CarNoCanJudge.class);
    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();

    private ICarNoCanDecide carNoCanDecide;

    public CarNoCanJudge() {
        this(new CarNoCanDecide());
    }

    public CarNoCanJudge(ICarNoCanDecide carNoCanDecide) {
        super(ConfigUtils.getSysDefine().getNoticeCanFaultTriggerContinueCount(),
            ConfigUtils.getSysDefine().getNoticeCanFaultTriggerTimeoutMillisecond(),
            ConfigUtils.getSysDefine().getNoticeCanNormalTriggerContinueCount(),
            ConfigUtils.getSysDefine().getNoticeCanNormalTriggerTimeoutMillisecond());
        this.carNoCanDecide = carNoCanDecide;
    }

    @Override
    protected String buildRedisKey() {
        return "vehCache.qy.can.notice";
    }

    @Override
    protected boolean ignore(final ImmutableMap<String, String> data) {
        final String vehicleId = data.get(DataKey.VEHICLE_ID);
        String timeString = data.get(DataKey.TIME);
        if (StringUtils.isBlank(timeString)) {
            LOG.info("vid:{} 无CAN放弃判定, 时间空白.", vehicleId);
            return true;
        }
        return false;
    }

    @Override
    protected State parseState(final ImmutableMap<String, String> data) {
        boolean hasCan = carNoCanDecide.hasCan(data);
        if (!hasCan) {
            //无can
            return State.BEGIN;
        } else if (hasCan) {
            //有can
            return State.END;
        }
        return State.UNKNOWN;
    }

    @Override
    protected @NotNull ImmutableMap<String, String> initBeginNotice(
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString) {

        String time = data.get(DataKey.TIME);
        LOG.trace("VID:{} 无CAN开始通知初始化", vehicleId);
        return new ImmutableMap.Builder<String, String>()
            .put("vid", vehicleId)
            .put("msgType", NoticeType.NO_CAN_VEH)
            .put("msgId", UUID.randomUUID().toString())
            .put("stime", time)
            .put("smileage", getTotalMileageString(vehicleId, data))
            .put("slocation", DataUtils.buildLocation(data))
            .build();
    }

    @Override
    protected Map<String, String> buildBeginNotice(
        @NotNull final ImmutableMap<String, String> data,
        final int count,
        final long timeout,
        @NotNull final String vehicleId,
        Map<String, String> notice) {

        notice.put(NOTICE_STATUS_KEY, NOTICE_START_STATUS);
        notice.put("sdelay", String.valueOf(getBeginTriggerTimeoutMillisecond() / 1000));
        notice.put("noticetime", DataUtils.buildFormatTime());

        try {
            VEHICLE_CACHE.putField(
                vehicleId,
                NoticeType.NO_CAN_VEH,
                ImmutableMap.copyOf(notice)
            );
        } catch (ExecutionException e) {
            LOG.warn("VID:{} 更新 VEHICLE_CACHE 失败", vehicleId);
        }

        LOG.debug("VID:{} 无CAN开始通知发送 MSGID:{}", vehicleId, notice.get("msgId"));
        return notice;
    }

    @Override
    protected @NotNull ImmutableMap<String, String> initEndNotice(
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString) {

        String time = data.get(DataKey.TIME);
        LOG.trace("VID:{} 无CAN结束通知初始化", vehicleId);
        return new ImmutableMap.Builder<String, String>()
            .put("etime", time)
            .put("emileage", getTotalMileageString(vehicleId, data))
            .put("elocation", DataUtils.buildLocation(data))
            .build();
    }

    @Override
    protected Map<String, String> buildEndNotice(
        @NotNull final ImmutableMap<String, String> data,
        int count,
        long timeout,
        @NotNull final String vehicleId,
        final Map<String, String> notice) {

        notice.put(NOTICE_STATUS_KEY, NOTICE_END_STATUS);
        notice.put("edelay", String.valueOf(getEndTriggerTimeoutMillisecond() / 1000));
        notice.put("noticetime", DataUtils.buildFormatTime());

        try {
            VEHICLE_CACHE.delField(
                vehicleId,
                NoticeType.NO_CAN_VEH
            );
            LOG.info("VID:{} CAN正常, 删除缓存.", vehicleId);
        } catch (ExecutionException e) {
            LOG.error("VID:{} 删除CAN正常通知缓存异常", vehicleId);
        }

        LOG.info("VID:{} 无CAN结束通知发送 MSGID:{}", vehicleId, notice.get("msgId"));
        return notice;
    }

    //region 内部方法
    @NotNull
    private String getTotalMileageString(@NotNull String vid, ImmutableMap<String, String> data) {
        try {
            // 总里程
            final String totalMileage = data.get(DataKey._2202_TOTAL_MILEAGE);
            if (NumberUtils.isDigits(totalMileage)) {
                return totalMileage;
            } else {
                return VEHICLE_CACHE.getTotalMileageString(vid, "0");
            }
        } catch (ExecutionException e) {
            LOG.warn("获取总里程出错", e);
        }
        return "0";
    }
    //endregion
}
