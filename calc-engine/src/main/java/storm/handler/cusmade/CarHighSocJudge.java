package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dto.notice.VehicleAlarmNotice;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.util.ConfigUtils;
import storm.util.DataUtils;

import java.util.UUID;

/**
 * SOC过高预警
 * 修改[ xzj ]：
 * 重构相关逻辑
 *
 * @author 于心沼, xzj
 */
public class CarHighSocJudge extends AbstractVehicleDelaySwitchJudge<VehicleAlarmNotice> {
    private static final Logger LOG = LoggerFactory.getLogger(CarHighSocJudge.class);

    public CarHighSocJudge() {
        super(ConfigUtils.getSysDefine().getNoticeSocHighBeginTriggerContinueCount(),
            ConfigUtils.getSysDefine().getNoticeSocHighBeginTriggerTimeoutMillisecond(),
            ConfigUtils.getSysDefine().getNoticeSocHighEndTriggerContinueCount(),
            ConfigUtils.getSysDefine().getNoticeSocHighEndTriggerTimeoutMillisecond());
    }

    @Override
    public String buildRedisKey() {
        return "vehCache.qy.soc.high.notice";
    }

    /**
     * 检查数据有效性
     *
     * @param data
     * @return
     */
    @Override
    public boolean ignore(final ImmutableMap<String, String> data) {
        final String socString = data.get(DataKey._7615_STATE_OF_CHARGE);
        if (!NumberUtils.isDigits(socString)) {
            return true;
        }
        return false;
    }

    @Override
    public NoticeState parseState(final ImmutableMap<String, String> data) {
        final int soc = Integer.parseInt(data.get(DataKey._7615_STATE_OF_CHARGE));
        if (soc >= ConfigUtils.getSysDefine().getNoticeSocHighBeginTriggerThreshold()) {
            //SOC过高开始阈值
            return NoticeState.BEGIN;
        } else if (soc < ConfigUtils.getSysDefine().getNoticeSocHighEndTriggerThreshold()) {
            //SOC过高结束阈值
            return NoticeState.END;
        }
        return NoticeState.UNKNOWN;
    }

    @NotNull
    @Override
    protected VehicleAlarmNotice initBeginNotice(
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString) {

        LOG.trace("VID:{} SOC过高开始首帧缓存初始化", vehicleId);
        String location = DataUtils.buildLocation(data);

        // 初始化通知
        VehicleAlarmNotice notice = new VehicleAlarmNotice();
        notice.setVid(vehicleId);
        notice.setMsgId(UUID.randomUUID().toString());
        notice.setMsgType(NoticeType.SOC_HIGH_NOTICE);
        notice.setStime(platformReceiverTimeString);
        notice.setLocation(location);
        notice.setSlocation(location);
        notice.setSthreshold(String.valueOf(ConfigUtils.getSysDefine().getNoticeSocHighBeginTriggerThreshold()));
        notice.setSsoc(data.get(DataKey._7615_STATE_OF_CHARGE));

        return notice;
    }

    @Override
    protected void buildBeginNotice(
        @NotNull final ImmutableMap<String, String> data,
        final int count,
        final long timeout,
        @NotNull final String vehicleId,
        @NotNull final VehicleAlarmNotice notice) {

        LOG.debug("VID:{} SOC过高开始通知发送 MSGID:{}", vehicleId, notice.getMsgId());

        notice.setScontinue(count + "");
        notice.setSlazy(timeout + "");
        notice.setNoticeTime(DataUtils.buildFormatTime());
    }

    @Override
    protected @NotNull void initEndNotice(
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString,
        @NotNull final VehicleAlarmNotice notice) {

        LOG.trace("VID:{} SOC过高结束首帧初始化", vehicleId);

        notice.setEtime(platformReceiverTimeString);
        notice.setElocation(DataUtils.buildLocation(data));
        notice.setEthreshold(ConfigUtils.getSysDefine().getNoticeSocHighEndTriggerThreshold() + "");
        notice.setEsoc(data.get(DataKey._7615_STATE_OF_CHARGE));
    }

    @Override
    protected void buildEndNotice(
        @NotNull final ImmutableMap<String, String> data,
        final int count,
        final long timeout,
        @NotNull final String vehicleId,
        @NotNull final VehicleAlarmNotice notice) {

        LOG.debug("VID:{} SOC过高结束通知发送 MSGID:{}", vehicleId, notice.getMsgId());

        notice.setEcontinue(count + "");
        notice.setElazy(timeout + "");
        notice.setNoticeTime(DataUtils.buildFormatTime());
    }

}
