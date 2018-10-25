package storm.handler.cusmade;

import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.FormatConstant;
import storm.constant.RedisConstant;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.util.*;

import java.text.ParseException;
import java.util.*;
/**
 * @author 于心沼
 * SOC过高预警
 */
public class CarHighSocJudge  {
    private static final ParamsRedisUtil PARAMS_REDIS_UTIL = ParamsRedisUtil.getInstance();
    private static final Logger logger = LoggerFactory.getLogger(CarHighSocJudge.class);
    private static final JsonUtils GSON_UTILS = JsonUtils.getInstance();
    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();
    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static final int REDIS_DB_INDEX = 6;
    private static final String REDIS_TABLE_NAME = "vehCache.qy.high.soc.notice";
    private static final String STATUS_KEY = "status";


    //region<<..........................................................3个缓存.........................................................>>
    /**
     * SOC过高通知开始缓存
     * 类型：
     * Map<vid, Map<vid,socNotice>>
     */
    public static Map<String, Map<String, String>> vidHighSocNotice = new HashMap<>();

    /**
     * SOC 过高计数器
     * 类型：
     * Map<vid, soc过高帧数计数>
     */
    public static Map<String, Integer> vidHighSocCount = new HashMap<>();
    /**
     * SOC 正常计数器
     * 类型：
     * Map<vid, soc正常帧数计数>
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
    private static Long highSocFaultIntervalMillisecond = (long) 60000;
    /**
     * SOC过高恢复确认延时, 默认0秒，即立刻触发.
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

    // 实例初始化代码块，从redis加载lowHigh车辆
    {
        JEDIS_POOL_UTILS.useResource(jedis -> {

            final String select = jedis.select(REDIS_DB_INDEX);
            if (!RedisConstant.Select.OK.equals(select)) {
                return;
            }
            final Map<String, String> notices = jedis.hgetAll(REDIS_TABLE_NAME);
            for (String vid : notices.keySet()) {

                logger.info("从Redis还原lowHigh车辆信息:[{}]", vid);

                final String json = notices.get(vid);
                final Map<String, String> notice = GSON_UTILS.fromJson(
                        json,
                        new TypeToken<TreeMap<String, String>>() {
                        }.getType());

                try {
                    final String statusString = notice.get(STATUS_KEY).toString();
                    final int statusValue = NumberUtils.toInt(statusString);
                    final AlarmStatus status = AlarmStatus.parseOf(statusValue);

                    if (AlarmStatus.Start == status || AlarmStatus.Continue == status) {
                        vidHighSocNotice.put((String) notice.get("vid"), notice);
                    }
                } catch (Exception ignore) {
                    logger.warn("初始化告警异常", ignore);
                }
            }
        });
    }


    /**
     * @param data 车辆数据
     * @return 如果产生过高电量通知, 则填充通知, 否则为空集合.
     */
    public Map<String, String> processFrame(@NotNull Map<String, String> data) {

        //检查数据有效性
        if (dataIsInvalid(data)){
            return null;
        }

        String vid = data.get(DataKey.VEHICLE_ID);
        String socString = data.get(DataKey._7615_STATE_OF_CHARGE);
        int socNum = Integer.parseInt(socString);

        //当有这辆车的数据过来的时候, 检查是处于(开始、结束、未知)三者之中的哪一种即可,
        // 如果是"未知"状态, 通过 redis 查一次, 有"开始"的缓存, 那么状态就可以确定为开始,
        // 没有缓存, 那么状态就可以确定为"结束", 至此就不存在"未知"的状态了.
        String status = null;
        if (null != vidHighSocNotice.get(vid)){
            status = vidHighSocNotice.get(vid).get(STATUS_KEY);
            if (AlarmStatus.Init.equals(status)){
                JEDIS_POOL_UTILS.useResource(jedis -> {
                    final String select = jedis.select(REDIS_DB_INDEX);
                    if (!RedisConstant.Select.OK.equals(select)) {
                        return;
                    }
                    final String json = jedis.hget(REDIS_TABLE_NAME, vid);
                    final Map<String, String> notice = GSON_UTILS.fromJson(
                            json,
                            new TypeToken<TreeMap<String, String>>() {
                            }.getType());
                    if (null == notice){
                        vidHighSocNotice.get(vid).put(STATUS_KEY,"3");
                    }else{
                        final String statusString = notice.get(STATUS_KEY);
                        final int statusValue = NumberUtils.toInt(statusString);
                        final AlarmStatus alarmStatus = AlarmStatus.parseOf(statusValue);

                        if (AlarmStatus.Start == alarmStatus){
                            vidHighSocNotice.get(vid).put(STATUS_KEY,"1");
                        }
                    }
                });
            }
        }

        // 检验SOC是否大于过高开始阈值
        if (socNum > highSocAlarm_StartThreshold) {
            //soc过高开始通知
            return getSocHighNotice(data);
        } else {
            //soc过高结束通知
            return getSocNormalNotice(data);
        }
    }

    /**
     * 判断实时报文数据是否为无效数据
     * @param data 实时报文数据
     * @return
     */
    private boolean dataIsInvalid(Map<String, String> data){
        if (MapUtils.isEmpty(data)) {
            return true;
        }
        String vid = data.get(DataKey.VEHICLE_ID);
        String timeString = data.get(DataKey._9999_PLATFORM_RECEIVE_TIME);
        String socString = data.get(DataKey._7615_STATE_OF_CHARGE);
        if (StringUtils.isBlank(vid)
                || StringUtils.isEmpty(timeString)
                || !StringUtils.isNumeric(timeString)) {
            return true;
        }
        if (StringUtils.isEmpty(socString)
                || !StringUtils.isNumeric(socString)) {
            return true;
        }
        return false;
    }


    /**
     * SOC小于soc过低开始阈值时做的操作
     */
    private Map<String, String> getSocHighNotice(Map<String, String> data){
        String vid = data.get(DataKey.VEHICLE_ID);
        String timeString = data.get(DataKey._9999_PLATFORM_RECEIVE_TIME);
        String longitudeString = data.get(DataKey._2502_LONGITUDE);
        String latitudeString = data.get(DataKey._2503_LATITUDE);
        String location = DataUtils.buildLocation(longitudeString, latitudeString);
        String socString = data.get(DataKey._7615_STATE_OF_CHARGE);
        int socNum = Integer.parseInt(socString);

        Long currentTimeMillis = System.currentTimeMillis();
        String noticeTime = DateFormatUtils.format(currentTimeMillis, FormatConstant.DATE_FORMAT);
        // 返回的通知消息
        Map<String, String> result = new HashMap<>();

        //SOC正常帧数记录值清空
        vidHighNormSoc.remove(vid);

        //此车之前是否为SOC过高状态
        final Map<String, String> highSocNotice = vidHighSocNotice.getOrDefault(vid, new TreeMap<>());
        // 0-初始化, 1-异常开始, 2-异常持续, 3-异常结束
        String status = highSocNotice.getOrDefault("status", "0");
        if ("0".equals(status) && "3".equals(status)) {
            return null;
        }

        if(MapUtils.isEmpty(highSocNotice)) {
            highSocNotice.put("msgType", NoticeType.SOC_HIGH_NOTICE);
            highSocNotice.put("msgId", UUID.randomUUID().toString());
            highSocNotice.put("vid", vid);
            vidHighSocNotice.put(vid, highSocNotice);

            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                logger.info("VID[{}]SOC首帧缓存初始化", vid);
            });
        }

        //过高SOC帧数加1
        final int highSocCount = vidHighSocCount.getOrDefault(vid, 0) + 1;
        vidHighSocCount.put(vid, highSocCount);

        PARAMS_REDIS_UTIL.autoLog(vid, ()->{
            logger.info("VID[{}]判定为SOC过低第[{}]次", vid, highSocCount);
        });

        // 记录连续SOC过高状态开始时的信息
        if(1 == highSocCount) {
            highSocNotice.put("stime", timeString);
            highSocNotice.put("location", location);
            highSocNotice.put("slocation", location);
            highSocNotice.put("sthreshold", String.valueOf(highSocAlarm_StartThreshold));
            highSocNotice.put("ssoc", String.valueOf(socNum));

            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                logger.info("VID[{}]SOC过低首帧更新", vid);
            });
        }

        //过高soc帧数是否超过阈值
        if(highSocCount < highSocFaultJudgeNum) {
            return null;
        }

        final Long firstLowSocTime;
        try {
            firstLowSocTime = DateUtils.parseDate(highSocNotice.get("stime"), new String[]{FormatConstant.DATE_FORMAT}).getTime();
        } catch (ParseException e) {
            logger.warn("解析开始时间异常", e);
            highSocNotice.put("stime", timeString);
            return null;
        }

        //故障时间是否超过阈值
        if (currentTimeMillis - firstLowSocTime <= highSocFaultIntervalMillisecond) {
            return null;
        }

        //记录连续SOC过高状态确定时的信息
        highSocNotice.put("status", "1");
        highSocNotice.put("slazy", String.valueOf(highSocFaultIntervalMillisecond));
        highSocNotice.put("noticeTime", noticeTime);

        String json = JSON_UTILS.toJson(highSocNotice);
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            jedis.hset(REDIS_TABLE_NAME, vid, json);
        });

        result = highSocNotice;

        PARAMS_REDIS_UTIL.autoLog(vid, ()->{
            logger.info("VID[{}]SOC异常通知发送[{}]", vid, highSocNotice.get("msgId"));
        });

        return result;
    }

    private Map<String,String> getSocNormalNotice(Map<String,String> data){

        String vid = data.get(DataKey.VEHICLE_ID);
        String timeString = data.get(DataKey._9999_PLATFORM_RECEIVE_TIME);
        String longitudeString = data.get(DataKey._2502_LONGITUDE);
        String latitudeString = data.get(DataKey._2503_LATITUDE);
        String location = DataUtils.buildLocation(longitudeString, latitudeString);
        String socString = data.get(DataKey._7615_STATE_OF_CHARGE);
        int socNum = Integer.parseInt(socString);

        Long currentTimeMillis = System.currentTimeMillis();
        String noticeTime = DateFormatUtils.format(currentTimeMillis, FormatConstant.DATE_FORMAT);

        //SOC过高帧数记录值清空
        vidHighSocCount.remove(vid);

        //此车之前是否为SOC过高状态
        final Map<String, String> normalSocNotice = vidHighSocNotice.get(vid);
        if(null == normalSocNotice) {
            return null;
        }

        // 0-初始化, 1-异常开始, 2-异常持续, 3-异常结束
        String status = normalSocNotice.getOrDefault("status", "0");
        if ("1".equals(status) && "2".equals(status)) {
            return null;
        }

        //如果大于过高结束阈值，则清空SOC正常帧数记录值，并返回
        if (socNum > highSocAlarm_EndThreshold){
            //SOC正常帧数记录值清空
            vidHighNormSoc.remove(vid);
            return null;
        }

        //正常SOC帧数加1
        final int normalSocCount = vidHighNormSoc.getOrDefault(vid, 0) + 1;
        vidHighNormSoc.put(vid, normalSocCount);

        PARAMS_REDIS_UTIL.autoLog(vid, ()->{
            logger.info("VID[{}]判定为SOC正常第[{}]次", vid, normalSocCount);
        });

        //记录首帧正常报文信息（即soc过低结束时信息）
        if(1 == normalSocCount) {
            normalSocNotice.put("etime", timeString);
            normalSocNotice.put("elocation", location);
            normalSocNotice.put("ethreshold", String.valueOf(highSocAlarm_StartThreshold));
            normalSocNotice.put("esoc", String.valueOf(socNum));

            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                logger.info("VID[{}]SOC正常首帧初始化", vid);
            });
        }

        //正常soc帧数是否超过阈值
        if(normalSocCount < highSocNormalJudgeNum) {
            return null;
        }

        final Long firstNormalSocTime;
        try {
            firstNormalSocTime = DateUtils.parseDate(normalSocNotice.get("etime"), new String[]{FormatConstant.DATE_FORMAT}).getTime();
        } catch (ParseException e) {
            logger.warn("解析结束时间异常", e);
            normalSocNotice.put("etime", timeString);
            return null;
        }

        //正常时间是否超过阈值
        if (currentTimeMillis - firstNormalSocTime <= highSocNormalIntervalMillisecond) {
            return null;
        }

        //记录连续SOC正常状态确定时的信息
        normalSocNotice.put("status", "3");
        normalSocNotice.put("elazy", String.valueOf(highSocNormalIntervalMillisecond));
        normalSocNotice.put("noticeTime", noticeTime);

        vidHighSocNotice.remove(vid);
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            jedis.hdel(REDIS_TABLE_NAME, vid);
        });

        PARAMS_REDIS_UTIL.autoLog(vid, ()->{
            logger.info("VID[{}]SOC正常通知发送", vid, normalSocNotice.get("msgId"));
        });

        return normalSocNotice;
    }


    /**
     * 告警状态
     */
    private enum AlarmStatus {

        /**
         * 初始
         */
        Init(0),

        /**
         * 开始
         */
        Start(1),

        /**
         * 持续
         */
        Continue(2),

        /**
         * 结束
         */
        End(3),;

        public final int value;

        AlarmStatus(int value) {
            this.value = value;
        }

        @Contract(pure = true)
        public static AlarmStatus parseOf(int value) {
            if (Init.value == value) {
                return Init;
            }
            if (Start.value == value) {
                return Start;
            }
            if (Continue.value == value) {
                return Continue;
            }
            if (End.value == value) {
                return End;
            }

            throw new UnknownFormatConversionException("无法识别的选项");
        }

        @Contract(pure = true)
        public boolean equals(int value) {
            return this.value == value;
        }

        @Contract("null -> false")
        public boolean equals(String name) {
            return this.name().equals(name);
        }
    }

    //region   以下为6个可配置变量的get和set方法

    public static ParamsRedisUtil getParamsRedisUtil() {
        return PARAMS_REDIS_UTIL;
    }

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


    //endregion


}
