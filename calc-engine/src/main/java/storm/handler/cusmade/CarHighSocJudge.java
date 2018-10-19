package storm.handler.cusmade;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.FormatConstant;
import storm.dao.DataToRedis;
import storm.handler.ctx.Recorder;
import storm.handler.ctx.RedisRecorder;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.util.DataUtils;
import storm.util.JsonUtils;
import storm.util.ParamsRedisUtil;

import java.text.ParseException;
import java.util.*;

public class CarHighSocJudge {
    private static final ParamsRedisUtil PARAMS_REDIS_UTIL = ParamsRedisUtil.getInstance();
    private static final Logger logger = LoggerFactory.getLogger(CarHighSocJudge.class);
    private static final JsonUtils GSON_UTILS = JsonUtils.getInstance();

    //region<<..........................................................数据库相关配置..........................................................>>
    DataToRedis redis;
    private Recorder recorder;
    static String socHighRedisKeys = "vehCache.qy.high.soc.notice";
    static int db = 6;
    //endregion

    //region<<..........................................................3个缓存.........................................................>>
    /**
     * SOC过高通知开始缓存
     */
    public static Map<String, Map<String, Object>> vidHighSocNotice = new HashMap<>();

    /**
     * SOC 过高计数器
     */
    public static Map<String, Integer> vidHighSocCount = new HashMap<>();
    /**
     * SOC 正常计数器
     */
    public static Map<String, Integer> vidHighNormSoc = new HashMap<>();
    //endregion

    //region<<.........................................................6个可以配置的阈值..........................................................>>
    /**
     * SOC过高确认帧数
     */
    private static int highSocFaultJudgeNum = 3;
    private static int highSocNormalJudgeNum = 1;

    /**
     * SOC过高触发确认延时, 默认1分钟.
     */
    private static Long highSocFaultIntervalMillisecond = (long) 30000;
    /**
     * SOC过高恢复确认延时, 默认1分钟.
     */
    private static Long highSocNormalIntervalMillisecond = (long) 0;

    /**
     * SOC过高告警触发阈值
     */
    private static int highSocAlarm_StartThreshold = 90;
    /**
     * SOC过高告警结束阈值
     */
    private static int highSocAlarm_EndThreshold = 80;
    //endregion

    // 实例初始化代码块，从redis加载highSoc车辆
    {
        redis = new DataToRedis();
        recorder = new RedisRecorder(redis);
        restartInit(true);
    }

    /**
     * @param data 车辆数据
     * @return 如果产生低电量通知, 则填充通知, 否则为空集合.
     */
    //@NotNull   这个的作用，有时间搞清楚。
    public List<Map<String, Object>> processFrame(@NotNull Map<String, String> data) {

        if (MapUtils.isEmpty(data)) {
            return null;
        }

        final String vid = data.get(DataKey.VEHICLE_ID);
        final String timeString = data.get(DataKey._9999_PLATFORM_RECEIVE_TIME);
        if (StringUtils.isBlank(vid)
                || StringUtils.isEmpty(timeString)
                || !StringUtils.isNumeric(timeString)) {
            return null;
        }

        final String longitudeString = data.get(DataKey._2502_LONGITUDE);
        final String latitudeString = data.get(DataKey._2503_LATITUDE);
        final String location = DataUtils.buildLocation(longitudeString, latitudeString);

        final Long currentTimeMillis = System.currentTimeMillis();
        final String noticeTime = DateFormatUtils.format(
                currentTimeMillis,
                FormatConstant.DATE_FORMAT);

        // 返回的通知消息
        final List<Map<String, Object>> result = new LinkedList<>();

        final String socString = data.get(DataKey._7615_STATE_OF_CHARGE);
        if (StringUtils.isEmpty(socString)
                || !StringUtils.isNumeric(socString)) {
            return null;
        }
        final int socNum = Integer.parseInt(socString);

        // 检验SOC是否大于过高开始阈值
        if (socNum > highSocAlarm_StartThreshold) {

            //SOC正常帧数记录值清空
            vidHighNormSoc.remove(vid);

            //此车之前是否SOC过高状态
            final Map<String, Object> highSocNotice = vidHighSocNotice.getOrDefault(vid, new TreeMap<>());
            // 0-初始化, 1-异常开始, 2-异常持续, 3-异常结束
            int status = (int)highSocNotice.getOrDefault("status", 0);
            if (status != 0 && status != 3) {
                return null;
            }

            if(MapUtils.isEmpty(highSocNotice)) {
                highSocNotice.put("msgType", NoticeType.SOC_HIGH_ALARM);
                highSocNotice.put("msgId", UUID.randomUUID().toString());
                highSocNotice.put("vid", vid);
                vidHighSocNotice.put(vid, highSocNotice);

                PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                    logger.info("VID[{}]SOC_HIGH首帧缓存初始化", vid);
                });
            }

            //过高SOC帧数加1
            final int highSocCount = vidHighSocCount.getOrDefault(vid, 0) + 1;
            vidHighSocCount.put(vid, highSocCount);

            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                logger.info("VID[{}]判定为SOC过高第[{}]次", vid, highSocCount);
            });

            // 记录连续SOC过高状态开始时的信息
            if(1 == highSocCount) {
                highSocNotice.put("stime", timeString);
                highSocNotice.put("location", location);
                highSocNotice.put("slocation", location);
                highSocNotice.put("sthreshold", highSocAlarm_StartThreshold);
                highSocNotice.put("ssoc", socNum);

                PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                    logger.info("VID[{}]SOC过高首帧更新", vid);
                });
            }

            //过高soc帧数是否超过阈值
            if(highSocCount < highSocFaultJudgeNum) {
                return null;
            }

            final Long firstHighSocTime;
            try {
                firstHighSocTime = DateUtils
                        .parseDate(
                                highSocNotice.get("stime").toString(),
                                new String[]{FormatConstant.DATE_FORMAT})
                        .getTime();
            } catch (ParseException e) {
                logger.warn("解析开始时间异常", e);
                highSocNotice.put("stime", timeString);
                return null;
            }

            //故障时间是否超过阈值
            if (currentTimeMillis - firstHighSocTime <= highSocFaultIntervalMillisecond) {
                return null;
            }

            //记录连续SOC过高状态确定时的信息
            highSocNotice.put("status", 1);
            highSocNotice.put("slazy", highSocFaultIntervalMillisecond);
            highSocNotice.put("noticeTime", noticeTime);

            //把soc过高开始通知存储到redis中
            recorder.save(db, socHighRedisKeys, vid, highSocNotice);

            result.add(highSocNotice);

            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                logger.info("VID[{}]SOC过高异常通知发送[{}]", vid, highSocNotice.get("msgId"));
            });
            
            return result;
        } else {

            //SOC过高帧数记录值清空
            vidHighSocCount.remove(vid);

            //此车之前是否为SOC过高状态
            final Map<String, Object> highSocNotice = vidHighSocNotice.get(vid);
            if(null == highSocNotice) {
                return null;
            }

            // 0-初始化, 1-异常开始, 2-异常持续, 3-异常结束
            int status = (int)highSocNotice.getOrDefault("status", 0);
            if (status != 1 && status != 2) {
                return null;
            }

            //检验SOC是否小于过高结束阈值(有点绕)
            if (socNum > highSocAlarm_EndThreshold){
                //SOC正常帧数记录值清空
                vidHighNormSoc.remove(vid);
                return null;
            }

            //正常SOC帧数加1
            final int normalSocCount = vidHighNormSoc.getOrDefault(vid, 0) + 1;
            vidHighNormSoc.put(vid, normalSocCount);

            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                logger.info("VID[{}]判定为SOC过高正常第[{}]次", vid, normalSocCount);
            });

            //记录首帧正常报文信息（即soc过低结束时信息）
            if(1 == normalSocCount) {
                highSocNotice.put("etime", timeString);
                highSocNotice.put("elocation", location);
                highSocNotice.put("ethreshold", highSocAlarm_StartThreshold);
                highSocNotice.put("esoc", socNum);

                PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                    logger.info("VID[{}]SOC过高正常首帧初始化", vid);
                });
            }

            //正常soc帧数是否超过阈值
            if(normalSocCount < highSocNormalJudgeNum) {
                return null;
            }

            final Long firstNormalSocTime;
            try {
                firstNormalSocTime = DateUtils.parseDate(highSocNotice.get("etime").toString(), new String[]{FormatConstant.DATE_FORMAT}).getTime();
            } catch (ParseException e) {
                logger.warn("解析结束时间异常", e);
                highSocNotice.put("etime", timeString);
                return result;
            }

            //正常时间是否超过阈值
            if (currentTimeMillis - firstNormalSocTime <= highSocNormalIntervalMillisecond) {
                return result;
            }

            //记录连续SOC正常状态确定时的信息
            highSocNotice.put("status", 3);
            highSocNotice.put("elazy", highSocNormalIntervalMillisecond);
            highSocNotice.put("noticeTime", noticeTime);

            vidHighSocNotice.remove(vid);
            recorder.del(db, socHighRedisKeys, vid);

            //返回soc过低结束通知
            result.add(highSocNotice);

            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                logger.info("VID[{}]SOC过高正常通知发送", vid, highSocNotice.get("msgId"));
            });

            return result;
        }
    }



    /**
     * 初始化highSoc通知的缓存
     * @param isRestart
     */
    void restartInit(boolean isRestart) {
        if (isRestart) {
            recorder.rebootInit(db, socHighRedisKeys, vidHighSocNotice);
        }
    }


    //以下为6个可配置变量的get和set方法

    public static int getHighSocFaultJudgeNum() {
        return highSocFaultJudgeNum;
    }

    public static void setHighSocFaultJudgeNum(int highSocFaultJudgeNum) {
        CarHighSocJudge.highSocFaultJudgeNum = highSocFaultJudgeNum;
    }

    public static int getHighSocNormalJudgeNum() {
        return highSocNormalJudgeNum;
    }

    public static void setHighSocNormalJudgeNum(int highSocNormalJudgeNum) {
        CarHighSocJudge.highSocNormalJudgeNum = highSocNormalJudgeNum;
    }

    public static Long getHighSocFaultIntervalMillisecond() {
        return highSocFaultIntervalMillisecond;
    }

    public static void setHighSocFaultIntervalMillisecond(Long highSocFaultIntervalMillisecond) {
        CarHighSocJudge.highSocFaultIntervalMillisecond = highSocFaultIntervalMillisecond;
    }

    public static Long getHighSocNormalIntervalMillisecond() {
        return highSocNormalIntervalMillisecond;
    }

    public static void setHighSocNormalIntervalMillisecond(Long highSocNormalIntervalMillisecond) {
        CarHighSocJudge.highSocNormalIntervalMillisecond = highSocNormalIntervalMillisecond;
    }

    public static int getHighSocAlarm_StartThreshold() {
        return highSocAlarm_StartThreshold;
    }

    public static void setHighSocAlarm_StartThreshold(int highSocAlarm_StartThreshold) {
        CarHighSocJudge.highSocAlarm_StartThreshold = highSocAlarm_StartThreshold;
    }

    public static int getHighSocAlarm_EndThreshold() {
        return highSocAlarm_EndThreshold;
    }

    public static void setHighSocAlarm_EndThreshold(int highSocAlarm_EndThreshold) {
        CarHighSocJudge.highSocAlarm_EndThreshold = highSocAlarm_EndThreshold;
    }
}
