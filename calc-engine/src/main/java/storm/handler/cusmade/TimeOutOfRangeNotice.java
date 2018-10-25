package storm.handler.cusmade;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.FormatConstant;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.util.DataUtils;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * @author: xzp
 * @date: 2018-06-15
 * @description:
 */
public final class TimeOutOfRangeNotice implements Serializable {

    private static final long serialVersionUID = -8735877631948355567L;

    private static final Logger LOG = LoggerFactory.getLogger(TimeOutOfRangeNotice.class);

    public static long DEFAULT_TIME_RANGE_MILLISECOND =  1000 * 60 * 10;

    private static long timeRangeMillisecond = DEFAULT_TIME_RANGE_MILLISECOND;

    @Contract(pure = true)
    public static long getTimeRangeMillisecond() {
        return timeRangeMillisecond;
    }

    public static void setTimeRangeMillisecond(final long timeRangeMillisecond) {
        TimeOutOfRangeNotice.timeRangeMillisecond = timeRangeMillisecond;
        LOG.info("时间数值异常范围被设定为 " + timeRangeMillisecond + " 毫秒");
    }

    /**
     * @param data 车辆数据
     * @return 如果有异常, 则设置vid, exception
     */
    @NotNull
    public final Map<String, String> process(@NotNull final Map<String, String> data) {
        final Map<String, String> result = new TreeMap<>();

        final String vid = data.get(DataKey.VEHICLE_ID);
        if(StringUtils.isBlank(vid)) {
            return result;
        }

        final String terminalTimeString = data.get(DataKey._2000_TERMINAL_COLLECT_TIME);

        // 时间有效性异常
        if(!NumberUtils.isDigits(terminalTimeString)) {
            return generateNotice(data, 1);
        }

        final long terminalTime;
        final long platformTime;

        final String platformTimeString = data.get(DataKey._9999_PLATFORM_RECEIVE_TIME);
        if(!NumberUtils.isDigits(platformTimeString)) {
            LOG.warn("VID:{} 平台接收时间格式错误[{}]", vid, platformTimeString);
            return result;
        }
        try {
            terminalTime = DateUtils.parseDate(terminalTimeString, new String[]{FormatConstant.DATE_FORMAT}).getTime();
            platformTime = DateUtils.parseDate(platformTimeString, new String[]{FormatConstant.DATE_FORMAT}).getTime();
        } catch (ParseException e) {
            LOG.warn("时间解析异常", e);
            LOG.warn("VID:{} 无效的时间格式: 2000=[{}], 9999=[{}]", vid, terminalTimeString, platformTimeString);
            return result;
        }

        // 数值异常
        if(Math.abs(platformTime - terminalTime) > getTimeRangeMillisecond()) {
            final Map<String, String> notice = generateNotice(data, 2);
            notice.put("timeRangeMillisecond", String.valueOf(timeRangeMillisecond));
            return notice;
        }

        return result;
    }

    /**
     * @param data 车辆数据
     * @param exceptionType 异常类型: 1-时间有效性异常, 2-时间数值异常
     * @return 异常通知
     */
    @NotNull
    private Map<String, String> generateNotice(@NotNull Map<String, String> data, int exceptionType) {
        // 车辆ID
        final String vid = data.get(DataKey.VEHICLE_ID);
        // 终端时间
        final String terminalTimeString = data.get(DataKey._2000_TERMINAL_COLLECT_TIME);
        // 平台时间
        final String platformTimeString = data.get(DataKey._9999_PLATFORM_RECEIVE_TIME);
        // 经度
        final String longitude = data.get(DataKey._2502_LONGITUDE);
        // 纬度
        final String latitude = data.get(DataKey._2503_LATITUDE);
        // 经纬度坐标
        final String location = DataUtils.buildLocation(longitude, latitude);

        final Map<String, String> notice = new TreeMap<>();
        notice.put("vid", vid);
        notice.put("msgType", NoticeType.TIME_EXCEPTION_VEH);
        notice.put("msgId", UUID.randomUUID().toString());
        notice.put("exceptionType", String.valueOf(exceptionType));
        notice.put("ttime", terminalTimeString);
        notice.put("ptime", platformTimeString);
        notice.put("location", location);

        return notice;
    }
}
