package storm.handler.cusmade;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.VehicleCache;
import storm.dto.notice.NoCanNotice;
import storm.protocol.CommandType;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.util.ConfigUtils;
import storm.util.DataUtils;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * @author: xzp, xzj
 * @date: 2018-06-14
 * @date 2019-1-2
 * @description: 无CAN车辆审计者
 */
public final class CarNoCanJudge extends AbstractVehicleDelaySwitchJudge<NoCanNotice> {
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
        final String msgType = data.get(DataKey.MESSAGE_TYPE);
        if(!CommandType.SUBMIT_REALTIME.equals(msgType)) {
            return true;
        }
        return false;
    }

    @Override
    protected NoticeState parseState(final ImmutableMap<String, String> data) {
        boolean hasCan = carNoCanDecide.hasCan(data);
        if (!hasCan) {
            //无can
            return NoticeState.BEGIN;
        } else if (hasCan) {
            //有can
            return NoticeState.END;
        }
        return NoticeState.UNKNOWN;
    }

    @NotNull
    @Override
    protected NoCanNotice initBeginNotice(
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString) {

        LOG.trace("VID:{} 无CAN开始通知初始化", vehicleId);

        NoCanNotice notice = new NoCanNotice();
        notice.setVid(vehicleId);
        notice.setMsgId(UUID.randomUUID().toString());
        notice.setMsgType(NoticeType.NO_CAN_VEH);
        notice.setStime(data.get(DataKey.TIME));
        notice.setSmileage(getTotalMileageString(vehicleId, data));
        notice.setSlocation(DataUtils.buildLocation(data));

        return notice;
    }

    @Override
    protected void buildBeginNotice(
        @NotNull final ImmutableMap<String, String> data,
        final int count,
        final long timeout,
        @NotNull final String vehicleId,
        @NotNull final NoCanNotice notice) {

        LOG.debug("VID:{} 无CAN开始通知发送 MSGID:{}", vehicleId, notice.getMsgId());
        String noticeTime = DataUtils.buildFormatTime();

        notice.setSdelay(String.valueOf(getBeginTriggerTimeoutMillisecond() / 1000));
        notice.setNoticeTime(noticeTime);
        //兼容旧的消息格式
        notice.setNoticetime(noticeTime);

        try {
            VEHICLE_CACHE.putField(
                vehicleId,
                NoticeType.NO_CAN_VEH,
                ImmutableMap.of(vehicleId, JSON.toJSONString(notice))
            );
        } catch (ExecutionException e) {
            LOG.warn("VID:{} 更新 VEHICLE_CACHE 失败", vehicleId);
        }
    }

    @Override
    protected NoCanNotice initEndNotice(
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString) {

        LOG.trace("VID:{} 无CAN结束通知初始化", vehicleId);

        NoCanNotice notice = new NoCanNotice();
        notice.setEtime(data.get(DataKey.TIME));
        notice.setEmileage(getTotalMileageString(vehicleId, data));
        notice.setElocation(DataUtils.buildLocation(data));

        return notice;
    }

    @Override
    protected void buildEndNotice(
        @NotNull final ImmutableMap<String, String> data,
        final int count,
        final long timeout,
        @NotNull final String vehicleId,
        @NotNull final NoCanNotice notice) {

        LOG.info("VID:{} 无CAN结束通知发送 MSGID:{}", vehicleId, notice.getMsgId());
        String noticeTime = DataUtils.buildFormatTime();

        notice.setEdelay(String.valueOf(getEndTriggerTimeoutMillisecond() / 1000));
        notice.setNoticeTime(noticeTime);
        //兼容旧的消息格式
        notice.setNoticetime(noticeTime);

        try {
            VEHICLE_CACHE.delField(
                vehicleId,
                NoticeType.NO_CAN_VEH
            );
            LOG.info("VID:{} CAN正常, 删除缓存.", vehicleId);
        } catch (ExecutionException e) {
            LOG.error("VID:{} 删除CAN正常通知缓存异常", vehicleId);
        }
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
