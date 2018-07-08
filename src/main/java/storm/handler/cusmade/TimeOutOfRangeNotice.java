package storm.handler.cusmade;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.FormatConstant;
import storm.system.DataKey;
import storm.util.DataUtils;
import storm.util.ParamsRedisUtil;

import java.text.ParseException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * @author: xzp
 * @date: 2018-06-15
 * @description:
 */
public final class TimeOutOfRangeNotice {

    private static final Logger logger = LoggerFactory.getLogger(TimeOutOfRangeNotice.class);
    private static final ParamsRedisUtil paramsRedisUtil = ParamsRedisUtil.getInstance();

    private static long timeRangeMillisecond = 1000 * 60 * 10;

    public static long getTimeRangeMillisecond() {
        return timeRangeMillisecond;
    }

    public static void setTimeRangeMillisecond(long timeRangeMillisecond) {
        TimeOutOfRangeNotice.timeRangeMillisecond = timeRangeMillisecond;
        logger.info("时间数值异常范围被设定为[" + timeRangeMillisecond + "]毫秒");
    }

    /**
     * @param data 车辆数据
     * @return 如果有异常, 则设置vid, exception
     */
    @NotNull
    public Map<String, String> process(@NotNull Map<String, String> data) {
        final Map<String, String> result = new TreeMap<>();

        final String vid = data.get(DataKey.VEHICLE_ID);
        if(StringUtils.isBlank(vid)) {
            return result;
        }

        if(paramsRedisUtil.isTraceVehicleId(vid)) {
            logger.warn("VID[" + vid + "]进入时间有效范围通知判定");
        }

        final String terminalTimeString = data.get(DataKey._2000_COLLECT_TIME);

        // 时间有效性异常
        if(StringUtils.isBlank(terminalTimeString)) {
            return generateNotice(data, 1);
        }

        final long terminalTime;
        final long platformTime;

        final String platformTimeString = data.get(DataKey._9999_SERVER_RECEIVE_TIME);
        try {
            terminalTime = DateUtils.parseDate(terminalTimeString, new String[]{FormatConstant.DATE_FORMAT}).getTime();
            platformTime = DateUtils.parseDate(platformTimeString, new String[]{FormatConstant.DATE_FORMAT}).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            return result;
        }

        // 数值异常
        if(Math.abs(platformTime - terminalTime) > getTimeRangeMillisecond()) {
            return generateNotice(data, 2);
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
        final String terminalTimeString = data.get(DataKey._2000_COLLECT_TIME);
        // 平台时间
        final String platformTimeString = data.get(DataKey._9999_SERVER_RECEIVE_TIME);
        // 经度
        final String longitude = data.get(DataKey._2502_LONGITUDE);
        // 纬度
        final String latitude = data.get(DataKey._2503_LATITUDE);
        // 经纬度坐标
        final String location = DataUtils.buildLocation(longitude, latitude);

        final Map<String, String> notice = new TreeMap<>();
        notice.put("vid", vid);
        notice.put("msgType", "TIME_EXCEPTION_VEH");
        notice.put("msgId", UUID.randomUUID().toString());
        notice.put("exceptionType", String.valueOf(exceptionType));
        notice.put("ttime", terminalTimeString);
        notice.put("ptime", platformTimeString);
        notice.put("location", location);

        return notice;
    }
}
