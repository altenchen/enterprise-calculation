package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
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

import static storm.cache.VehicleCache.REDIS_DB_INDEX;

public class CarLowSocJudge {
    private static final ParamsRedisUtil PARAMS_REDIS_UTIL = ParamsRedisUtil.getInstance();
    private static final Logger logger = LoggerFactory.getLogger(CarLowSocJudge.class);
    private static final JsonUtils GSON_UTILS = JsonUtils.getInstance();

    //region<<..........................................................数据库相关配置..........................................................>>
    DataToRedis redis;
    private Recorder recorder;
    static String socRedisKeys = "vehCache.qy.soc.notice";
    static int db = 6;
    static int topn = 20;
    //endregion

    //region<<..........................................................3个缓存.........................................................>>
    /**
     * SOC过低通知开始缓存
     */
    public static Map<String, Map<String, Object>> vidSocNotice = new HashMap<>();

    /**
     * SOC 过低计数器
     */
    public static Map<String, Integer> vidLowSocCount = new HashMap<>();
    /**
     * SOC 正常计数器
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
    private static Long lowSocFaultIntervalMillisecond = (long) 30000;
    /**
     * SOC过低恢复确认延时, 默认1分钟.
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
        restartInit(true);
    }

    /**
     * @param data 车辆数据
     * @return 如果产生低电量通知, 则填充通知, 否则为空集合.
     */
    @NotNull
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

        // 检验SOC是否小于过低开始阈值
        if (socNum < lowSocAlarm_StartThreshold) {

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
            recorder.save(db, socRedisKeys, vid, lowSocNotice);

            result.add(lowSocNotice);

            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                logger.info("VID[{}]SOC异常通知发送[{}]", vid, lowSocNotice.get("msgId"));
            });

            //查找附近补电车
            try {
                final double longitude = Double.parseDouble(NumberUtils.isNumber(longitudeString) ? longitudeString : "0") / 1000000.0;
                final double latitude = Double.parseDouble(NumberUtils.isNumber(latitudeString) ? latitudeString : "0") / 1000000.0;
                Map<String, Object> chargeMap = chargeCarNotice(vid, longitude, latitude);
                if (MapUtils.isNotEmpty(chargeMap)) {
                    result.add(chargeMap);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            return result;
        } else {

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

            //检验SOC是否大于过低结束阈值
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
                return result;
            }

            //正常时间是否超过阈值
            if (currentTimeMillis - firstNormalSocTime <= lowSocNormalIntervalMillisecond) {
                return result;
            }

            //记录连续SOC正常状态确定时的信息
            normalSocNotice.put("status", 3);
            normalSocNotice.put("elazy", lowSocNormalIntervalMillisecond);
            normalSocNotice.put("noticeTime", noticeTime);

            vidSocNotice.remove(vid);
            recorder.del(db, socRedisKeys, vid);

            //返回soc过低结束通知
            result.add(normalSocNotice);

            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                logger.info("VID[{}]SOC正常通知发送", vid, normalSocNotice.get("msgId"));
            });

            return result;
        }
    }


    /**
     * 查找附近补电车 保存到 redis 中
     *
     * @param vid
     * @param longitude
     * @param latitude
     */
    private Map<String, Object> chargeCarNotice(String vid, double longitude, double latitude) {
        Map<String, FillChargeCar> fillvidgps = SysRealDataCache.getChargeCarCache();
        Map<Double, FillChargeCar> chargeCarInfo = findNearFill(longitude, latitude, fillvidgps);

        if (null != chargeCarInfo) {
            Map<String, Object> chargeMap = new TreeMap<>();
            List<Map<String, Object>> chargeCars = new LinkedList<>();
            Map<String, String> topnCars = new TreeMap<>();
            int cts = 0;
            for (Map.Entry<Double, FillChargeCar> entry : chargeCarInfo.entrySet()) {
                cts++;
                if (cts > topn) {
                    break;
                }
                double distance = entry.getKey();
                FillChargeCar chargeCar = entry.getValue();

                //save to redis map
                Map<String, Object> jsonMap = new TreeMap<>();
                jsonMap.put("vid", chargeCar.vid);
                jsonMap.put("LONGITUDE", chargeCar.longitude);
                jsonMap.put("LATITUDE", chargeCar.latitude);
                jsonMap.put("lastOnline", chargeCar.lastOnline);
                jsonMap.put("distance", distance);

                String jsonString = GSON_UTILS.toJson(jsonMap);
                topnCars.put("" + cts, jsonString);
                //send to kafka map
                Map<String, Object> kMap = new TreeMap<>();
                kMap.put("vid", chargeCar.vid);
                kMap.put("location", chargeCar.longitude + "," + chargeCar.latitude);
                kMap.put("lastOnline", chargeCar.lastOnline);
                kMap.put("gpsDis", distance);
                kMap.put("ranking", cts);
                kMap.put("running", chargeCar.running);

                chargeCars.add(kMap);
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

    /**
     * 查找附件补电车
     *
     * @param longitude  经度
     * @param latitude   纬度
     * @param fillvidgps 缓存的补电车 vid [经度，纬度]
     * @return 距离， 补电车
     */
    private Map<Double, FillChargeCar> findNearFill(double longitude, double latitude, Map<String, FillChargeCar> fillvidgps) {
        //
        if (null == fillvidgps || fillvidgps.size() == 0) {
            return null;
        }

        if ((0 == longitude && 0 == latitude)
                || Math.abs(longitude) > 180
                || Math.abs(latitude) > 180) {
            return null;
        }
        /**
         * 按照此种方式加上远近排名的话可能会有 bug,
         * 此方法假设 任意两点的 车子gps距离 的double值不可能相等
         * 出现的 概率极低，暂时忽略
         */

        Map<Double, FillChargeCar> carSortMap = new TreeMap<>();

        for (Map.Entry<String, FillChargeCar> entry : fillvidgps.entrySet()) {
//            String fillvid = entry.getKey();
            FillChargeCar chargeCar = entry.getValue();
            double distance = GpsUtil.getDistance(longitude, latitude, chargeCar.longitude, chargeCar.latitude);
            carSortMap.put(distance, chargeCar);
        }
        if (carSortMap.size() > 0) {
            return carSortMap;
        }
        return null;
    }

    /**
     * 初始化lowSoc通知的缓存
     * @param isRestart
     */
    void restartInit(boolean isRestart) {
        if (isRestart) {
            recorder.rebootInit(db, socRedisKeys, vidSocNotice);
        }
    }


    //以下为6个可配置变量的get和set方法
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


}
