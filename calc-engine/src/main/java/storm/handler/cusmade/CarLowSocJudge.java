package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dto.notice.LowSocNotice;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.util.ConfigUtils;
import storm.util.DataUtils;

import java.util.UUID;

/**
 * @author 徐志鹏
 * SOC过低预警
 */
class CarLowSocJudge extends AbstractVehicleDelaySwitchJudge<LowSocNotice> {

    private static final Logger LOG = LoggerFactory.getLogger(CarLowSocJudge.class);

    public CarLowSocJudge() {
        super(ConfigUtils.getSysDefine().getNoticeSocLowBeginTriggerContinueCount(),
            ConfigUtils.getSysDefine().getNoticeSocLowBeginTriggerTimeoutMillisecond(),
            ConfigUtils.getSysDefine().getNoticeSocLowEndTriggerContinueCount(),
            ConfigUtils.getSysDefine().getNoticeSocLowEndTriggerTimeoutMillisecond());
    }

    @Override
    protected String buildRedisKey() {
        return "vehCache.qy.soc.notice";
    }

    @Override
    protected boolean ignore(final ImmutableMap<String, String> data) {
        final String socString = data.get(DataKey._7615_STATE_OF_CHARGE);
        if (!NumberUtils.isDigits(socString)) {
            return true;
        }
        return false;
    }

    @Override
    protected NoticeState parseState(final ImmutableMap<String, String> data) {
        final int soc = Integer.parseInt(data.get(DataKey._7615_STATE_OF_CHARGE));
        if (soc <= ConfigUtils.getSysDefine().getNoticeSocLowBeginTriggerThreshold()) {
            //SOC过高开始阈值
            return NoticeState.BEGIN;
        } else if (soc > ConfigUtils.getSysDefine().getNoticeSocLowEndTriggerThreshold()) {
            //SOC过高结束阈值
            return NoticeState.END;
        }
        return NoticeState.UNKNOWN;
    }

    @NotNull
    @Override
    protected LowSocNotice initBeginNotice(
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString) {

        LOG.trace("VID:{} SOC过低开始首帧缓存初始化", vehicleId);
        String location = DataUtils.buildLocation(data);

        LowSocNotice notice = new LowSocNotice();
        notice.setVid(vehicleId);
        notice.setMsgId(UUID.randomUUID().toString());
        notice.setMsgType(NoticeType.SOC_LOW_NOTICE);
        notice.setStime(platformReceiverTimeString);
        notice.setLocation(location);
        notice.setSlocation(location);
        notice.setSthreshold(ConfigUtils.getSysDefine().getNoticeSocLowBeginTriggerThreshold() + "");
        notice.setSsoc(data.get(DataKey._7615_STATE_OF_CHARGE));
        // 兼容性处理, 暂留
        notice.setLowSocThreshold(ConfigUtils.getSysDefine().getNoticeSocLowEndTriggerThreshold() + "");

        return notice;
    }

    @Override
    protected void buildBeginNotice(
        @NotNull final ImmutableMap<String, String> data,
        final int count,
        final long timeout,
        @NotNull final String vehicleId,
        @NotNull final LowSocNotice notice) {

        LOG.debug("VID:{} SOC过低开始通知发送 MSGID:{}", vehicleId, notice.getMsgId());

        notice.setScontinue(count + "");
        notice.setSlazy(timeout + "");
        notice.setNoticeTime(DataUtils.buildFormatTime());
    }

    @Override
    protected @NotNull LowSocNotice initEndNotice(
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString) {

        LOG.trace("VID:{} SOC过低结束首帧初始化", vehicleId);

        LowSocNotice notice = new LowSocNotice();
        notice.setEtime(platformReceiverTimeString);
        notice.setElocation(DataUtils.buildLocation(data));
        notice.setEthreshold(ConfigUtils.getSysDefine().getNoticeSocLowEndTriggerThreshold() + "");
        notice.setEsoc(data.get(DataKey._7615_STATE_OF_CHARGE));

        return notice;
    }

    @Override
    protected void buildEndNotice(
        @NotNull final ImmutableMap<String, String> data,
        final int count,
        final long timeout,
        @NotNull final String vehicleId,
        @NotNull final LowSocNotice notice) {

        LOG.debug("VID:{} SOC过低结束通知发送 MSGID:{}", vehicleId, notice.getMsgId());

        notice.setEcontinue(count + "");
        notice.setElazy(timeout + "");
        notice.setNoticeTime(DataUtils.buildFormatTime());
    }
}
