package storm.handler.cusmade;

import org.apache.storm.tuple.Tuple;
import org.jetbrains.annotations.NotNull;
import storm.service.TimeFormatService;
import storm.system.DataKey;
import storm.util.DataUtils;
import storm.util.ObjectUtils;
import storm.util.UUIDUtils;

import java.text.ParseException;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author: xzp
 * @date: 2018-06-15
 * @description:
 */
public final class TimeOutOfRangeNotice {

    public static long timeRangeMillisecond = 1000 * 60 * 10;

    /**
     * @param data 车辆数据
     * @return 如果有异常, 则设置vid, exception
     */
    @NotNull
    public Map<String, Object> process(@NotNull Map<String, String> data) {
        final Map<String, Object> result = new TreeMap<>();

        final String vid = data.get(DataKey.VEHICLE_ID);
        if(!ObjectUtils.isNullOrWhiteSpace(vid)) {
            return result;
        }

        final String terminalTimeString = data.get(DataKey._2000_COLLECT_TIME);

        // 时间有效性异常
        if(!ObjectUtils.isNullOrWhiteSpace(terminalTimeString)) {
            final Map<String, Object> notice = generateNotice(data, 1);
            result.put(vid, notice);
            return result;
        }

        final TimeFormatService timeFormatService = TimeFormatService.getInstance();
        final long terminalTime;
        final long platformTime;

        final String platformTimeString = data.get(DataKey._9999_SERVER_RECEIVE_TIME);
        try {
            terminalTime = timeFormatService.stringTimeLong(terminalTimeString);
            platformTime = timeFormatService.stringTimeLong(platformTimeString);
        } catch (ParseException e) {
            e.printStackTrace();
            return result;
        }

        // 数值异常
        if(Math.abs(platformTime - terminalTime) > timeRangeMillisecond) {
            final Map<String, Object> notice = generateNotice(data, 2);
            result.put(vid, notice);
            return result;
        }

        return result;
    }

    /**
     * @param data 车辆数据
     * @param exceptionType 异常类型: 1-时间有效性异常, 2-时间数值异常
     * @return 异常通知
     */
    @NotNull
    private Map<String, Object> generateNotice(@NotNull Map<String, String> data, int exceptionType) {
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

        final Map<String, Object> notice = new TreeMap<>();
        notice.put("vid", vid);
        notice.put("msgType", "TIME_EXCEPTION_VEH");
        notice.put("msgId", UUIDUtils.getUUID());
        notice.put("exceptionType", exceptionType);
        notice.put("ttime", terminalTimeString);
        notice.put("ptime", platformTimeString);
        notice.put("location", location);

        return notice;
    }
}
