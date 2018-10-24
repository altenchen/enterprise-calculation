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
import storm.cache.SysRealDataCache;
import storm.constant.FormatConstant;
import storm.constant.RedisConstant;
import storm.dao.DataToRedis;
import storm.dto.FillChargeCar;
import storm.handler.ctx.Recorder;
import storm.handler.ctx.RedisRecorder;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.util.*;

import java.text.ParseException;
import java.util.*;
/**
 * @author 于心沼
 * SOC过低预警
 */
public class CarLowSocJudge {
    private static final ParamsRedisUtil PARAMS_REDIS_UTIL = ParamsRedisUtil.getInstance();
    private static final Logger logger = LoggerFactory.getLogger(CarLowSocJudge.class);
    private static final JsonUtils GSON_UTILS = JsonUtils.getInstance();
    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();
    private static final int REDIS_DB_INDEX = 6;
    /**
     * 出于兼容性考虑暂留, 已存储到车辆缓存<code>VehicleCache</code>中
     */
    private static final String REDIS_TABLE_NAME = "vehCache.qy.soc.notice";
    private static final String STATUS_KEY = "status";

    //region<<..........................................................数据库相关配置..........................................................>>

    DataToRedis redis;
    private Recorder recorder;
    static int topn = 20;
    //endregion

    //region<<..........................................................3个缓存.........................................................>>
    /**
     * SOC过低通知开始缓存
     * 类型：
     * Map<vid, Map<vid,socNotice>>
     */
    public static Map<String, Map<String, Object>> vidSocNotice = new HashMap<>();

    /**
     * SOC 过低计数器
     * 类型：
     * Map<vid, soc过低帧数计数>
     */
    public static Map<String, Integer> vidLowSocCount = new HashMap<>();
    /**
     * SOC 正常计数器
     * 类型：
     * Map<vid, soc正常帧数计数>
     */
    public static Map<String, Integer> vidNormSoc = new HashMap<>();
    //endregion

    //region<<.........................................................6个可以配置的阈值..........................................................>>
    /**
     * SOC过低确认帧数
     */
    private static int lowSocFaultJudgeNum = 3;
    private static int lowSocNormalJudgeNum = 1;

    /**
     * SOC过低触发确认延时, 默认1分钟.
     */
    private static Long lowSocFaultIntervalMillisecond = (long) 60000;
    /**
     * SOC过低恢复确认延时, 默认0秒，即立刻触发.
     */
    private static Long lowSocNormalIntervalMillisecond = (long) 0;

    /**
     * SOC过低告警触发阈值
     */
    private static int lowSocAlarm_StartThreshold = 10;
    /**
     * SOC过低告警结束阈值
     */
    private static int lowSocAlarm_EndThreshold = 20;
    //endregion

    // 实例初始化代码块，从redis加载lowSoc车辆
    {
        redis = new DataToRedis();
        recorder = new RedisRecorder(redis);
//        restartInit(true);

        JEDIS_POOL_UTILS.useResource(jedis -> {

            final String select = jedis.select(REDIS_DB_INDEX);
            if (!RedisConstant.Select.OK.equals(select)) {
                return;
            }
            //这里难道不也是把数据全部加载到内存中了吗？
            final Map<String, String> notices = jedis.hgetAll(REDIS_TABLE_NAME);
            for (String vid : notices.keySet()) {

                logger.info("从Redis还原lowSoc车辆信息:[{}]", vid);

                final String json = notices.get(vid);
                final Map<String, Object> notice = GSON_UTILS.fromJson(
                        json,
                        new TypeToken<TreeMap<String, Object>>() {
                        }.getType());

                try {
                    final String statusString = notice.get(STATUS_KEY).toString();
                    final int statusValue = NumberUtils.toInt(statusString);
                    final AlarmStatus status = AlarmStatus.parseOf(statusValue);

                    if (AlarmStatus.Start == status || AlarmStatus.Continue == status) {
                        vidSocNotice.put((String) notice.get("vid"), notice);
                    }
                } catch (Exception ignore) {
                    logger.warn("初始化告警异常", ignore);
                }
            }
        });
    }


    /**
     * @param data 车辆数据
     * @return 如果产生低电量通知, 则填充通知, 否则为空集合.
     */
    public List<Map<String, Object>> processFrame(@NotNull Map<String, String> data) {

        //检查数据有效性
        if (dataIsInvalid(data)){
            return null;
        }

        String vid = data.get(DataKey.VEHICLE_ID);
        String longitudeString = data.get(DataKey._2502_LONGITUDE);
        String latitudeString = data.get(DataKey._2503_LATITUDE);
        String socString = data.get(DataKey._7615_STATE_OF_CHARGE);
        int socNum = Integer.parseInt(socString);

        //soc过低开始通知和附近补电车信息
        List<Map<String, Object>> result = new LinkedList<>();
        Map<String, Object> socLowNotice;
        //soc过低结束通知
        Map<String, Object> socNormalNotice;

        // 检验SOC是否小于过低开始阈值
        if (socNum < lowSocAlarm_StartThreshold) {

            socLowNotice = getSocLowNotice(data);
            //发送soc过低开始通知时，获取附近补电车信息，一并发送
            if (socLowNotice != null){
                result.add(socLowNotice);
                try {
                    final double longitude = Double.parseDouble(NumberUtils.isNumber(longitudeString) ? longitudeString : "0") / 1000000.0;
                    final double latitude = Double.parseDouble(NumberUtils.isNumber(latitudeString) ? latitudeString : "0") / 1000000.0;
                    Map<String, Object> chargeMap = getNoticesOfChargeCars(vid, longitude, latitude);
                    if (MapUtils.isNotEmpty(chargeMap)) {
                        result.add(chargeMap);
                    }
                } catch (Exception e) {
                    logger.warn("获取补电车信息的时出现异常，位置在CarLowSocJudge类");
                    logger.warn(e.getMessage());
                }
            }
            return result;
        } else {
            socNormalNotice = getSocNormalNotice(data);
            if (MapUtils.isNotEmpty(socNormalNotice)){
                result.add(socNormalNotice);
            }
            return result;
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
    private Map<String, Object> getSocLowNotice(Map<String, String> data){
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
        Map<String, Object> result = new HashMap<>();

        //SOC正常帧数记录值清空
        vidNormSoc.remove(vid);

        //此车之前是否SOC过低状态
        final Map<String, Object> lowSocNotice = vidSocNotice.getOrDefault(vid, new TreeMap<>());
        // 0-初始化, 1-异常开始, 2-异常持续, 3-异常结束
        int status = (int)lowSocNotice.getOrDefault("status", 0);
        if (status != 0 && status != 3) {
            return null;
        }

        if(MapUtils.isEmpty(lowSocNotice)) {
            lowSocNotice.put("msgType", NoticeType.SOC_ALARM);
            lowSocNotice.put("msgId", UUID.randomUUID().toString());
            lowSocNotice.put("vid", vid);
            vidSocNotice.put(vid, lowSocNotice);

            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                logger.info("VID[{}]SOC首帧缓存初始化", vid);
            });
        }

        //过低SOC帧数加1
        final int lowSocCount = vidLowSocCount.getOrDefault(vid, 0) + 1;
        vidLowSocCount.put(vid, lowSocCount);

        PARAMS_REDIS_UTIL.autoLog(vid, ()->{
            logger.info("VID[{}]判定为SOC过低第[{}]次", vid, lowSocCount);
        });

        // 记录连续SOC过低状态开始时的信息
        if(1 == lowSocCount) {
            lowSocNotice.put("stime", timeString);
            lowSocNotice.put("location", location);
            lowSocNotice.put("slocation", location);
            lowSocNotice.put("sthreshold", lowSocAlarm_StartThreshold);
            lowSocNotice.put("ssoc", socNum);
            // 兼容性处理, 暂留
            lowSocNotice.put("lowSocThreshold", lowSocAlarm_StartThreshold);

            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                logger.info("VID[{}]SOC过低首帧更新", vid);
            });
        }

        //过低soc帧数是否超过阈值
        if(lowSocCount < lowSocFaultJudgeNum) {
            return null;
        }

        final Long firstLowSocTime;
        try {
            firstLowSocTime = DateUtils
                    .parseDate(
                            lowSocNotice.get("stime").toString(),
                            new String[]{FormatConstant.DATE_FORMAT})
                    .getTime();
        } catch (ParseException e) {
            logger.warn("解析开始时间异常", e);
            lowSocNotice.put("stime", timeString);
            return null;
        }

        //故障时间是否超过阈值
        if (currentTimeMillis - firstLowSocTime <= lowSocFaultIntervalMillisecond) {
            return null;
        }

        //记录连续SOC过低状态确定时的信息
        lowSocNotice.put("status", 1);
        lowSocNotice.put("slazy", lowSocFaultIntervalMillisecond);
        lowSocNotice.put("noticeTime", noticeTime);

        //把soc过低开始通知存储到redis中
        recorder.save(REDIS_DB_INDEX, REDIS_TABLE_NAME, vid, lowSocNotice);

        result = lowSocNotice;

        PARAMS_REDIS_UTIL.autoLog(vid, ()->{
            logger.info("VID[{}]SOC异常通知发送[{}]", vid, lowSocNotice.get("msgId"));
        });


        return result;
    }

    private Map<String,Object> getSocNormalNotice(Map<String,String> data){

        String vid = data.get(DataKey.VEHICLE_ID);
        String timeString = data.get(DataKey._9999_PLATFORM_RECEIVE_TIME);
        String longitudeString = data.get(DataKey._2502_LONGITUDE);
        String latitudeString = data.get(DataKey._2503_LATITUDE);
        String location = DataUtils.buildLocation(longitudeString, latitudeString);
        String socString = data.get(DataKey._7615_STATE_OF_CHARGE);
        int socNum = Integer.parseInt(socString);

        Long currentTimeMillis = System.currentTimeMillis();
        String noticeTime = DateFormatUtils.format(currentTimeMillis, FormatConstant.DATE_FORMAT);

        //SOC过低帧数记录值清空
        vidLowSocCount.remove(vid);

        //此车之前是否为SOC过低状态
        final Map<String, Object> normalSocNotice = vidSocNotice.get(vid);
        if(null == normalSocNotice) {
            return null;
        }

        // 0-初始化, 1-异常开始, 2-异常持续, 3-异常结束
        int status = (int)normalSocNotice.getOrDefault("status", 0);
        if (status != 1 && status != 2) {
            return null;
        }

        //检验SOC是否大于过低结束阈值(有点绕)
        if (socNum < lowSocAlarm_EndThreshold){
            //SOC正常帧数记录值清空
            vidNormSoc.remove(vid);
            return null;
        }

        //正常SOC帧数加1
        final int normalSocCount = vidNormSoc.getOrDefault(vid, 0) + 1;
        vidNormSoc.put(vid, normalSocCount);

        PARAMS_REDIS_UTIL.autoLog(vid, ()->{
            logger.info("VID[{}]判定为SOC正常第[{}]次", vid, normalSocCount);
        });

        //记录首帧正常报文信息（即soc过低结束时信息）
        if(1 == normalSocCount) {
            normalSocNotice.put("etime", timeString);
            normalSocNotice.put("elocation", location);
            normalSocNotice.put("ethreshold", lowSocAlarm_StartThreshold);
            normalSocNotice.put("esoc", socNum);

            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                logger.info("VID[{}]SOC正常首帧初始化", vid);
            });
        }

        //正常soc帧数是否超过阈值
        if(normalSocCount < lowSocNormalJudgeNum) {
            return null;
        }

        final Long firstNormalSocTime;
        try {
            firstNormalSocTime = DateUtils.parseDate(normalSocNotice.get("etime").toString(), new String[]{FormatConstant.DATE_FORMAT}).getTime();
        } catch (ParseException e) {
            logger.warn("解析结束时间异常", e);
            normalSocNotice.put("etime", timeString);
            return null;
        }

        //正常时间是否超过阈值
        if (currentTimeMillis - firstNormalSocTime <= lowSocNormalIntervalMillisecond) {
            return null;
        }

        //记录连续SOC正常状态确定时的信息
        normalSocNotice.put("status", 3);
        normalSocNotice.put("elazy", lowSocNormalIntervalMillisecond);
        normalSocNotice.put("noticeTime", noticeTime);

        vidSocNotice.remove(vid);
        recorder.del(REDIS_DB_INDEX, REDIS_TABLE_NAME, vid);

        PARAMS_REDIS_UTIL.autoLog(vid, ()->{
            logger.info("VID[{}]SOC正常通知发送", vid, normalSocNotice.get("msgId"));
        });

        return normalSocNotice;
    }

    /**
     * 获得附近补电车的信息通知，并保存到 redis 中
     * @param vid
     * @param longitude
     * @param latitude
     */
    private Map<String, Object> getNoticesOfChargeCars(String vid, double longitude, double latitude) {
        Map<String, FillChargeCar> fillvidgps = SysRealDataCache.getChargeCarCache();
        FindChargeCarsOfNearby findChargeCars = new FindChargeCarsOfNearby();
        Map<Double, List<FillChargeCar>> chargeCarInfo = findChargeCars.findChargeCarsOfNearby(longitude, latitude, fillvidgps);

        if (null != chargeCarInfo) {
            Map<String, Object> chargeMap = new TreeMap<>();
            List<Map<String, Object>> chargeCars = new LinkedList<>();
            Map<String, String> topnCars = new TreeMap<>();
            int cts = 0;
            for (Map.Entry<Double, List<FillChargeCar>> entry : chargeCarInfo.entrySet()) {
                cts++;
                if (cts > topn) {
                    break;
                }
                double distance = entry.getKey();
                List<FillChargeCar> listOfChargeCar = entry.getValue();
                for (FillChargeCar ChargeCar : listOfChargeCar) {
                    //save to redis map
                    Map<String, Object> jsonMap = new TreeMap<>();
                    jsonMap.put("vid", ChargeCar.vid);
                    jsonMap.put("LONGITUDE", ChargeCar.longitude);
                    jsonMap.put("LATITUDE", ChargeCar.latitude);
                    jsonMap.put("lastOnline", ChargeCar.lastOnline);
                    jsonMap.put("distance", distance);

                    String jsonString = GSON_UTILS.toJson(jsonMap);
                    topnCars.put("" + cts, jsonString);
                    //send to kafka map
                    Map<String, Object> kMap = new TreeMap<>();
                    kMap.put("vid", ChargeCar.vid);
                    kMap.put("location", ChargeCar.longitude + "," + ChargeCar.latitude);
                    kMap.put("lastOnline", ChargeCar.lastOnline);
                    kMap.put("gpsDis", distance);
                    kMap.put("ranking", cts);
                    kMap.put("running", ChargeCar.running);

                    chargeCars.add(kMap);
                }
            }

            if (topnCars.size() > 0) {
                redis.saveMap(topnCars, 2, "charge-car-" + vid);
            }
            if (chargeCars.size() > 0) {

                chargeMap.put("vid", vid);
                chargeMap.put("msgType", NoticeType.CHARGE_CAR_NOTICE);
                chargeMap.put("location", longitude * 1000000 + "," + latitude * 1000000);
                chargeMap.put("fillChargeCars", chargeCars);
                return chargeMap;
            }
        }
        return null;
    }

//    /**
//     * 初始化lowSoc通知的缓存
//     * @param isRestart
//     */
//    void restartInit(boolean isRestart) {
//        if (isRestart) {
//            recorder.rebootInit(REDIS_DB_INDEX, REDIS_TABLE_NAME, vidSocNotice);
//        }
//    }
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

    public int getLowSocAlarm_StartThreshold() {
        return lowSocAlarm_StartThreshold;
    }

    public void setSocLowAlarm_StartThreshold(int lowSocAlarm_StartThreshold) {
        CarLowSocJudge.lowSocAlarm_StartThreshold = lowSocAlarm_StartThreshold;
    }

    public int getLowSocAlarm_EndThreshold() {
        return lowSocAlarm_EndThreshold;
    }

    public void setLowSocAlarm_EndThreshold(int lowSocAlarm_EndThreshold) {
        CarLowSocJudge.lowSocAlarm_EndThreshold = lowSocAlarm_EndThreshold;
    }

    public int getLowSocFaultJudgeNum() {
        return lowSocFaultJudgeNum;
    }

    public void setLowSocFaultJudgeNum(int lowSocFaultJudgeNum) {
        CarLowSocJudge.lowSocFaultJudgeNum = lowSocFaultJudgeNum;
    }

    public int getLowSocNormalJudgeNum() {
        return lowSocNormalJudgeNum;
    }

    public void setLowSocNormalJudgeNum(int lowSocNormalJudgeNum) {
        CarLowSocJudge.lowSocNormalJudgeNum = lowSocNormalJudgeNum;
    }

    public Long getLowSocFaultIntervalMillisecond() {
        return lowSocFaultIntervalMillisecond;
    }

    public void setLowSocFaultIntervalMillisecond(Long lowSocFaultIntervalMillisecond) {
        CarLowSocJudge.lowSocFaultIntervalMillisecond = lowSocFaultIntervalMillisecond;
    }

    public Long getLowSocNormalIntervalMillisecond() {
        return lowSocNormalIntervalMillisecond;
    }

    public void setLowSocNormalIntervalMillisecond(Long lowSocNormalIntervalMillisecond) {
        CarLowSocJudge.lowSocNormalIntervalMillisecond = lowSocNormalIntervalMillisecond;
    }

    //endregion


}
