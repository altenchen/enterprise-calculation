package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.util.ConfigUtils;
import storm.util.DataUtils;

import java.util.Map;
import java.util.UUID;

/**
 * SOC过高预警
 * 修改[ xzj ]：
 * 重构相关逻辑
 *
 * @author 于心沼, xzj
 */
public class CarHighSocJudge extends AbstractVehicleDelaySwitchJudge {
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
    public State parseState(final ImmutableMap<String, String> data) {
        final int soc = Integer.parseInt(data.get(DataKey._7615_STATE_OF_CHARGE));
        if (soc >= ConfigUtils.getSysDefine().getNoticeSocHighBeginTriggerThreshold()) {
            //SOC过高开始阈值
            return State.BEGIN;
        } else if (soc < ConfigUtils.getSysDefine().getNoticeSocHighEndTriggerThreshold()) {
            //SOC过高结束阈值
            return State.END;
        }
        return State.UNKNOWN;
    }

    @Override
    protected @NotNull ImmutableMap<String, String> initBeginNotice(
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString) {

        final int soc = Integer.parseInt(data.get(DataKey._7615_STATE_OF_CHARGE));
        String location = DataUtils.buildLocation(data);
        LOG.trace("VID:{} SOC过高开始首帧缓存初始化", vehicleId);
        return new ImmutableMap.Builder<String, String>()
            .put("msgType", NoticeType.SOC_HIGH_NOTICE)
            .put("msgId", UUID.randomUUID().toString())
            .put("vid", vehicleId)
            .put("stime", platformReceiverTimeString)
            .put("location", location)
            .put("slocation", location)
            .put("sthreshold", String.valueOf(ConfigUtils.getSysDefine().getNoticeSocHighBeginTriggerThreshold()))
            .put("ssoc", String.valueOf(soc))
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
        notice.put("scontinue", String.valueOf(count));
        notice.put("slazy", String.valueOf(timeout));
        notice.put("noticeTime", DataUtils.buildFormatTime());

        LOG.debug("VID:{} SOC过高开始通知发送 MSGID:{}", vehicleId, notice.get("msgId"));
        return notice;
    }

    @Override
    protected @NotNull ImmutableMap<String, String> initEndNotice(
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString) {

        final int soc = Integer.parseInt(data.get(DataKey._7615_STATE_OF_CHARGE));
        String location = DataUtils.buildLocation(data);
        LOG.trace("VID:{} SOC过高结束首帧初始化", vehicleId);
        return new ImmutableMap.Builder<String, String>()
            .put("etime", platformReceiverTimeString)
            .put("elocation", location)
            .put("ethreshold", String.valueOf(ConfigUtils.getSysDefine().getNoticeSocHighEndTriggerThreshold()))
            .put("esoc", String.valueOf(soc))
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
        notice.put("econtinue", String.valueOf(count));
        notice.put("elazy", String.valueOf(timeout));
        notice.put("noticeTime", DataUtils.buildFormatTime());

        LOG.debug("VID:{} SOC过高结束通知发送 MSGID:{}", vehicleId, notice.get("msgId"));
        return notice;
    }

}
