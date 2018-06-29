package storm.handler.cusmade;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.SysRealDataCache;
import storm.dao.DataToRedis;
import storm.dto.FillChargeCar;
import storm.dto.IsSendNoticeCache;
import storm.handler.ctx.Recorder;
import storm.handler.ctx.RedisRecorder;
import storm.protocol.CommandType;
import storm.protocol.SUBMIT_LINKSTATUS;
import storm.protocol.SUBMIT_LOGIN;
import storm.service.TimeFormatService;
import storm.system.*;
import storm.util.*;

import java.text.ParseException;
import java.util.*;

/**
 * <p>
 * 临时处理 沃特玛的需求 处理简单实现方法
 * 后续抽象需求出来
 * 用其他通用的工具来代替
 * </p>
 *
 * @author 76304
 * <p>
 * 车辆规则处理
 */
public class CarRuleHandler implements InfoNotice {

    private static final Logger logger = LoggerFactory.getLogger(CarRuleHandler.class);
    private static final ConfigUtils configUtils = ConfigUtils.getInstance();
    private static final ParamsRedisUtil paramsRedisUtil = ParamsRedisUtil.getInstance();

    

    private Map<String, Integer> vidNoGps;
    private Map<String, Integer> vidNormalGps;
    /**
     * 无CAN计数器
     */
    private final Map<String, Integer> vidNoCanCount = new HashMap<>();
    /**
     * CAN正常计数器
     */
    private final Map<String, Integer> vidNormalCanCount = new HashMap<>();

    private Map<String, Integer> vidIgnite;
    private Map<String, Integer> vidShut;
    private Map<String, Double> igniteShutMaxSpeed;
    private Map<String, Double> lastSoc;
    private Map<String, Double> lastMile;

    /**
     * SOC过低阈值, 默认 10%
     */
    private static int socAlarm = 10;
    /**
     * SOC过低确认帧数
     */
    private static int lowSocJudgeNum = 3;
    /**
     * SOC过低确认延时, 默认1分钟.
     */
    private static Long lowsocIntervalMillisecond = (long) 60000;
    /**
     * SOC 过低计数器
     */
    private Map<String, Integer> vidLowSocCount = new HashMap<>();
    /**
     * SOC 正常计数器
     */
    private Map<String, Integer> vidNormSoc = new HashMap<>();
    /**
     * SOC过低通知开始到结束信息, 第一帧数据更新
     */
    private Map<String, Map<String, Object>> vidSocNotice = new HashMap<>();

    private Map<String, Integer> vidSpeedGtZero;
    private Map<String, Integer> vidSpeedZero;

    private Map<String, Integer> vidFlySt;
    private Map<String, Integer> vidFlyEd;
    private Map<String, Map<String, Object>> vidcanNotice;
    private Map<String, Map<String, Object>> vidIgniteShutNotice;
    private Map<String, Map<String, Object>> vidgpsNotice;
    private Map<String, Map<String, Object>> vidSpeedGtZeroNotice;
    private Map<String, Map<String, Object>> vidFlyNotice;
    private Map<String, Map<String, Object>> vidOnOffNotice;

    private Map<String, Map<String, Object>> vidLastDat;//vid和最后一帧数据的缓存
    private Map<String, Map<String,Object>> vidLockStatus;

    private Map<String, Long> lastTime;
    private Map<String, IsSendNoticeCache> vidIsSendNoticeCache;


    DataToRedis redis;
    private Recorder recorder;
    static String onOffRedisKeys;
    static String socRedisKeys;

    static TimeFormatService timeformat;
    static int topn;
    static long offlinetime = 600000;//600秒
    static int nogpsJudgeNum = 5;//5次
    static int hasgpsJudgeNum = 10;//10次
    static int nocanJudgeNum = 5;//5次
    static int hascanJudgeNum = 10;//10次
    static int mileHop = 20;//2公里 ，单位是0.1km
    static Long nogpsIntervalTime = (long) 10800000;//10800秒,1天
    static Long nocanIntervalTime = (long) 10800000;//10800秒，1天

    static int db = 6;
    static int socRule = 0;//1代表规则启用
    static int enableCanRule = 0;//1代表规则启用
    static int igniteRule = 0;//1代表规则启用
    static int gpsRule = 0;//1代表规则启用
    static int abnormalRule = 0;//1代表规则启用
    static int flyRule = 0;//1代表规则启用
    static int onoffRule = 0;//1代表规则启用
    static int mileHopRule = 0;//1代表规则启用
    static int enableTimeRule = 0;//1代表规则启用
    static int carLockStatueChangeJudgeRule = 0;//1代表规则启用

    private final CarNoCanJudge carNoCanJudge = new CarNoCanJudge();
    private final TimeOutOfRangeNotice timeOutOfRangeNotice = new TimeOutOfRangeNotice();

    private final CarLockStatusChangeJudge carLockStatusChangeJudge = new CarLockStatusChangeJudge();
    private CarLockStatusChangeJudge carLockStatusChange;

    //以下参数可以通过读取配置文件进行重置
    static {
        logger.warn("运行静态代码块，读取配置文件中值");
        timeformat = TimeFormatService.getInstance();
        topn = 20;
        onOffRedisKeys = "vehCache.qy.onoff.notice";
        socRedisKeys = "vehCache.qy.soc.notice";
        if (null != configUtils.sysDefine) {
            logger.warn("运行静态代码块，判断配置文件是否存在");
            String off = configUtils.sysDefine.getProperty(StormConfigKey.REDIS_OFFLINE_SECOND);
            if (!StringUtils.isEmpty(off)) {
                offlinetime = Long.parseLong(off) * 1000;
            }

            String value = configUtils.sysDefine.getProperty("sys.carlockstatus.rule");
            if (!StringUtils.isEmpty(value)) {
                carLockStatueChangeJudgeRule = Integer.parseInt(value);
                value = null;
            }

            value = configUtils.sysDefine.getProperty("sys.soc.rule");
            if (!StringUtils.isEmpty(value)) {
                socRule = Integer.parseInt(value);
                value = null;
            }

            value = configUtils.sysDefine.getProperty("sys.can.rule");
            if (!StringUtils.isEmpty(value)) {
                enableCanRule = Integer.parseInt(value);
                value = null;
            }

            value = configUtils.sysDefine.getProperty("sys.ignite.rule");
            if (!StringUtils.isEmpty(value)) {
                igniteRule = Integer.parseInt(value);
                value = null;
            }

            value = configUtils.sysDefine.getProperty("sys.gps.rule");
            if (!StringUtils.isEmpty(value)) {
                gpsRule = Integer.parseInt(value);
                value = null;
            }

            value = configUtils.sysDefine.getProperty("sys.abnormal.rule");
            if (!StringUtils.isEmpty(value)) {
                abnormalRule = Integer.parseInt(value);
                value = null;
            }

            value = configUtils.sysDefine.getProperty("sys.fly.rule");
            if (!StringUtils.isEmpty(value)) {
                flyRule = Integer.parseInt(value);
                value = null;
            }

            value = configUtils.sysDefine.getProperty("sys.onoff.rule");
            if (!StringUtils.isEmpty(value)) {
                onoffRule = Integer.parseInt(value);
                value = null;
            }

            value = configUtils.sysDefine.getProperty("sys.milehop.rule");
            if (!StringUtils.isEmpty(value)) {
                mileHopRule = Integer.parseInt(value);
                value = null;
            }

            value = configUtils.sysDefine.getProperty(SysDefine.SOC_JUDGE_TIME);
            if (!StringUtils.isEmpty(value)) {
                lowsocIntervalMillisecond = Long.parseLong(value);
                value = null;
            }

            value = configUtils.sysDefine.getProperty(SysDefine.SOC_JUDGE_NO);
            if (!StringUtils.isEmpty(value)) {
                lowSocJudgeNum = Integer.parseInt(value);
                value = null;
            }

            value = configUtils.sysDefine.getProperty(SysDefine.LT_ALARM_SOC);
            if (!StringUtils.isEmpty(value)) {
                socAlarm = Integer.parseInt(value);
                value = null;
            }

            value = configUtils.sysDefine.getProperty(SysDefine.SYS_TIME_RULE);
            if (!StringUtils.isEmpty(value)) {
                enableTimeRule = Integer.parseInt(value);
                value = null;
            }
            logger.warn("时间异常通知规则" + (enableTimeRule == 1 ? "启用" : "停用"));
        }
        init();
    }

    //以下参数可以通过读取redis定时进行重新加载
    static void init() {
        Object socVal = paramsRedisUtil.PARAMS.get("lt.alarm.soc");
        if (null != socVal) {
            socAlarm = (int) socVal;
        }
        Object lowsocNum = paramsRedisUtil.PARAMS.get("soc.judge.no");
        if (null != lowsocNum) {
            lowSocJudgeNum = (int) lowsocNum;
        }
        Object socJudgeTime = paramsRedisUtil.PARAMS.get("soc.judge.time");
        if (null != socJudgeTime) {
            lowsocIntervalMillisecond = ((int) socJudgeTime)*1L;
        }


        Object nocanJugyObj = paramsRedisUtil.PARAMS.get("can.novalue.continue.no");
        if (null != nocanJugyObj) {
            nocanJudgeNum = (int) nocanJugyObj;
        }
        Object hascanJugyObj = paramsRedisUtil.PARAMS.get("can.novalue.continue.no");
        if (null != hascanJugyObj) {
            hascanJudgeNum = (int) hascanJugyObj;
        }


        Object nogpsNum = paramsRedisUtil.PARAMS.get("gps.novalue.continue.no");
        if (null != nogpsNum) {
            nogpsJudgeNum = (int) nogpsNum;
        }
        Object hasgpsNum = paramsRedisUtil.PARAMS.get("gps.novalue.continue.no");
        if (null != hasgpsNum) {
            hasgpsJudgeNum = (int) hasgpsNum;
        }
        Object hopnum = paramsRedisUtil.PARAMS.get("mile.hop.num");
        if (null != hopnum) {
            mileHop = ((int) hopnum) * 10;
        }
        Object nogpsJudgeTime = paramsRedisUtil.PARAMS.get("gps.judge.time");
        if (null != nogpsJudgeTime) {
            nogpsIntervalTime = ((int) nogpsJudgeTime) * 1000L;
        }
        Object nocanJudgeTime = paramsRedisUtil.PARAMS.get("can.judge.time");
        if (null != nocanJudgeTime) {
            nocanIntervalTime = ((int) nocanJudgeTime) * 1000L;
        }

        {
            final String ruleOverride = paramsRedisUtil.PARAMS.getOrDefault(
                SysDefine.RULE_OVERRIDE,
                SysDefine.RULE_OVERRIDE_VALUE_DEFAULT).toString();
            if(!StringUtils.isBlank(ruleOverride)) {
                if(SysDefine.RULE_OVERRIDE_VALUE_DEFAULT.equals(ruleOverride)) {
                    if(!(CarNoCanJudge.getCarNoCanDecide() instanceof CarNoCanDecideDefault)) {
                        CarNoCanJudge.setCarNoCanDecide(new CarNoCanDecideDefault());
                    }
                } else if(SysDefine.RULE_OVERRIDE_VALUE_JILI.equals(ruleOverride)) {
                    if(!(CarNoCanJudge.getCarNoCanDecide() instanceof CarNoCanDecideJili)) {
                        CarNoCanJudge.setCarNoCanDecide(new CarNoCanDecideJili());
                    }
                }
            }
        }
        {
            try {
                final String alarmCanFaultTriggerContinueCountString = paramsRedisUtil.PARAMS.get(
                    SysDefine.NOTICE_CAN_FAULT_TRIGGER_CONTINUE_COUNT).toString();
                final int alarmCanFaultTriggerContinueCount = Integer.parseInt(alarmCanFaultTriggerContinueCountString);
                if(alarmCanFaultTriggerContinueCount >= 0) {
                    CarNoCanJudge.setFaultTriggerContinueCount(alarmCanFaultTriggerContinueCount);
                }
            } catch (Exception ignored) {

            }
        }
        {
            try {
                final String alarmCanFaultTriggerTimeoutMillisecondString = paramsRedisUtil.PARAMS.get(
                    SysDefine.NOTICE_CAN_FAULT_TRIGGER_TIMEOUT_MILLISECOND).toString();
                final long alarmCanFaultTriggerTimeoutMillisecond = Long.parseLong(alarmCanFaultTriggerTimeoutMillisecondString);
                if(alarmCanFaultTriggerTimeoutMillisecond >= 0) {
                    CarNoCanJudge.setFaultTriggerTimeoutMillisecond(alarmCanFaultTriggerTimeoutMillisecond);
                }
            } catch (Exception ignored) {

            }
        }
        {
            try {
                final String alarmCanNormalTriggerContinueCountString = paramsRedisUtil.PARAMS.get(
                    SysDefine.NOTICE_CAN_NORMAL_TRIGGER_CONTINUE_COUNT).toString();
                final int alarmCanNormalTriggerContinueCount = Integer.parseInt(alarmCanNormalTriggerContinueCountString);
                if(alarmCanNormalTriggerContinueCount >= 0) {
                    CarNoCanJudge.setNormalTriggerContinueCount(alarmCanNormalTriggerContinueCount);
                }
            } catch (Exception ignored) {

            }
        }
        {
            try {
                final String alarmCanNormalTriggerTimeoutMillisecondString = paramsRedisUtil.PARAMS.get(
                    SysDefine.NOTICE_CAN_NORMAL_TRIGGER_TIMEOUT_MILLISECOND).toString();
                final long alarmCanNormalTriggerTimeoutMillisecond = Long.parseLong(alarmCanNormalTriggerTimeoutMillisecondString);
                if(alarmCanNormalTriggerTimeoutMillisecond >= 0) {
                    CarNoCanJudge.setNormalTriggerTimeoutMillisecond(alarmCanNormalTriggerTimeoutMillisecond);
                }
            } catch (Exception ignored) {

            }
        }
        {
            try {
                final String noticeTimeRangeAbsMillisecondString = paramsRedisUtil.PARAMS.get(
                    SysDefine.NOTICE_TIME_RANGE_ABS_MILLISECOND).toString();
                final long noticeTimeRangeAbsMillisecond = Long.parseLong(noticeTimeRangeAbsMillisecondString);
                if(noticeTimeRangeAbsMillisecond >= 0) {
                    TimeOutOfRangeNotice.setTimeRangeMillisecond(noticeTimeRangeAbsMillisecond);
                }
            } catch (Exception ignored) {

            }
        }
    }

    {
        vidNoGps = new HashMap<String, Integer>();
        vidNormalGps = new HashMap<String, Integer>();
        vidIgnite = new HashMap<String, Integer>();
        vidShut = new HashMap<String, Integer>();
        igniteShutMaxSpeed = new HashMap<String, Double>();
        lastMile = new HashMap<String, Double>();
        lastSoc = new HashMap<String, Double>();
        vidcanNotice = new HashMap<String, Map<String, Object>>();
        vidIgniteShutNotice = new HashMap<String, Map<String, Object>>();
        vidgpsNotice = new HashMap<String, Map<String, Object>>();

        vidSpeedGtZero = new HashMap<String, Integer>();
        vidSpeedZero = new HashMap<String, Integer>();
        vidSpeedGtZeroNotice = new HashMap<String, Map<String, Object>>();

        vidFlySt = new HashMap<String, Integer>();
        vidFlyEd = new HashMap<String, Integer>();
        vidFlyNotice = new HashMap<String, Map<String, Object>>();
        vidOnOffNotice = new HashMap<String, Map<String, Object>>();
        vidLastDat = new HashMap<String, Map<String, Object>>();
        lastTime = new HashMap<String, Long>();
        vidIsSendNoticeCache = new HashMap<String, IsSendNoticeCache>();
        vidLockStatus = new HashMap<String, Map<String, Object>>();

        redis = new DataToRedis();
        recorder = new RedisRecorder(redis);
        restartInit(true);
        carLockStatusChange = new CarLockStatusChangeJudge();
    }

    public static void rebulid() {
        paramsRedisUtil.rebulid();
        init();
    }

    /**
     * 生成通知
     *
     * @param data
     * @return
     */
    @NotNull
    public List<Map<String, Object>> generateNotices(@NotNull Map<String, String> data) {
        // 为下面的方法做准备，生成相应的容器。
        final List<Map<String, Object>> list = new LinkedList<>();

        // 验证data的有效性
        if (MapUtils.isEmpty(data)
                || !data.containsKey(DataKey.VEHICLE_ID)
                || !data.containsKey(DataKey.TIME)) {
            return list;
        }

        String vid = data.get(DataKey.VEHICLE_ID);
        if (StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(data.get(DataKey.TIME))) {
            return list;
        }

        lastTime.put(vid, System.currentTimeMillis());

        if(paramsRedisUtil.isTraceVehicleId(vid)) {
            logger.trace("VID[" + vid + "]进入车辆规则处理");
        }

        Map<String, Object> canJudge = null;
        Map<String, Object> igniteJudge = null;
        Map<String, Object> gpsJudge = null;
        Map<String, Object> abnormalJudge = null;
        Map<String, Object> flyJudge = null;
        Map<String, Object> onOffJudge = null;
        Map<String, Object> mileHopJudge = null;
        Map<String, Object> lockStatueChange = null;
        //3、如果规则启用了，则把dat放到相应的处理方法中。将返回结果放到list中，返回。

        if (1 == carLockStatueChangeJudgeRule) {
            lockStatueChange = carLockStatusChangeJudge.carLockStatueChangeJudge(data,vidLockStatus);
            if (!MapUtils.isEmpty(lockStatueChange)) {
                list.add(lockStatueChange);
            }
        }

        if (1 == socRule) {
            //lowsoc(data)返回一个map，里面有vid和通知消息（treeMap）
            // SOC 过低
            final List<Map<String, Object>> socJudges = lowSoc(data);
            if (!CollectionUtils.isEmpty(socJudges)) {
                list.addAll(socJudges);
            }
        }
        if (1 == enableCanRule) {
            // 无CAN车辆
            canJudge = carNoCanJudge.processFrame(data);
            if (!MapUtils.isEmpty(canJudge)) {
                list.add(canJudge);
            }
        }
        if (1 == igniteRule) {
            // 点火熄火
            igniteJudge = igniteOrShut(data);
            if (!MapUtils.isEmpty(igniteJudge)) {
                list.add(igniteJudge);
            }
        }
        if (1 == gpsRule) {
            // 为定位车辆
            gpsJudge = noGps(data);
        }
        if (1 == abnormalRule) {
            // 异常用车
            abnormalJudge = abnormalCar(data);
        }
        if (1 == flyRule) {
            // ????
            flyJudge = flySe(data);
        }
        if (1 == onoffRule) {
            // ???
            onOffJudge = onOffline(data);
        }
        if (1 == mileHopRule) {
            // 里程跳变处理
            mileHopJudge = mileHopHandle(data);
        }
        if(1 == enableTimeRule) {
            final Map<String, String> timeRangeJudge = timeOutOfRangeNotice.process(data);
            if (!MapUtils.isEmpty(timeRangeJudge)) {
                list.add(new TreeMap<>(timeRangeJudge));
            }
        }
        if (!MapUtils.isEmpty(gpsJudge)) {
            list.add(gpsJudge);
        }
        if (!MapUtils.isEmpty(abnormalJudge)) {
            list.add(abnormalJudge);
        }
        if (!MapUtils.isEmpty(flyJudge)) {
            list.add(flyJudge);
        }
        if (!MapUtils.isEmpty(onOffJudge)) {
            list.add(onOffJudge);
        }
        if (!MapUtils.isEmpty(mileHopJudge)) {
            list.add(mileHopJudge);
        }

        return list;
    }

    //soc<30 后面抽象成通用的规则
    /**
     * soc 过低
     */
    private List<Map<String, Object>> lowSoc(Map<String, String> dat) {
        if (MapUtils.isEmpty(dat)) {
            return null;
        }

        String vid = dat.get(DataKey.VEHICLE_ID);
        String timeString = dat.get(DataKey.TIME);
        if (StringUtils.isBlank(vid)
            || StringUtils.isEmpty(timeString)
            || !StringUtils.isNumeric(timeString)) {
            return null;
        }

        final String longitudeString = dat.get(DataKey._2502_LONGITUDE);
        final String latitudeString = dat.get(DataKey._2503_LATITUDE);
        final String location = DataUtils.buildLocation(longitudeString, latitudeString);

        final Date date = new Date();
        String noticeTime = timeformat.toDateString(date);

        // 返回的通知消息
        final List<Map<String, Object>> result = new LinkedList<>();

        final String socString = dat.get(DataKey._7615_STATE_OF_CHARGE);
        if (StringUtils.isEmpty(socString)
            || !StringUtils.isNumeric(socString)) {
            return null;
        }
        final int socNum = Integer.parseInt(socString);

        // 想判断是一个车辆是否为低电量，不能根据一个报文就下结论，而是要连续多个报文都是报低电量才行。
        // 其他的判断也都是类似的。
        if (socNum < socAlarm) {

            vidNormSoc.remove(vid);

            final int lowSocCount = vidLowSocCount.getOrDefault(vid, 0) + 1;
            vidLowSocCount.put(vid, lowSocCount);

            paramsRedisUtil.autoLog(vid, v->{
                logger.info("VID[{}]判定为SOC过低第[{}]次", v, lowSocCount);
            });

            final Map<String, Object> lowSocNotice = vidSocNotice.getOrDefault(vid, new TreeMap<>());
            if(MapUtils.isEmpty(lowSocNotice)) {
                lowSocNotice.put("msgType", "SOC_ALARM");
                lowSocNotice.put("msgId", UUID.randomUUID().toString());
                lowSocNotice.put("vid", vid);
                vidSocNotice.put(vid, lowSocNotice);

                paramsRedisUtil.autoLog(vid, v->{
                    logger.info("VID[{}]SOC首帧缓存初始化", v);
                });
            }

            // 0-初始化, 1-异常开始, 2-异常持续, 3-异常结束
            int status = (int)lowSocNotice.getOrDefault("status", 0);
            if (status != 0 && status != 3) {
                return null;
            }

            if(1 == lowSocCount) {
                lowSocNotice.put("stime", timeString);
                lowSocNotice.put("slocation", location);
                lowSocNotice.put("sthreshold", socAlarm);
                lowSocNotice.put("ssoc", socNum);
                // 兼容性处理, 暂留
                lowSocNotice.put("lowSocThreshold", socAlarm);

                paramsRedisUtil.autoLog(vid, v->{
                    logger.info("VID[{}]SOC过低首帧初始化", v);
                });
            }

            if(lowSocCount < lowSocJudgeNum) {
                return null;
            }

            final Long firstLowSocTime;
            try {
                firstLowSocTime = timeformat.stringTimeLong(lowSocNotice.get("stime").toString());
            } catch (ParseException e) {
                e.printStackTrace();
                return null;
            }

            final Long nowTime = date.getTime();
            if (nowTime - firstLowSocTime <= lowsocIntervalMillisecond) {
                return null;
            }

            lowSocNotice.put("status", 1);
            lowSocNotice.put("slazy", lowsocIntervalMillisecond);
            lowSocNotice.put("noticeTime", noticeTime);

            //把soc过低开始通知存储到redis中
            recorder.save(db, socRedisKeys, vid, lowSocNotice);

            result.add(lowSocNotice);

            paramsRedisUtil.autoLog(vid, v->{
                logger.info("VID[{}]SOC异常通知发送[{}]", v, lowSocNotice.get("msgId"));
            });

            //查找附近补电车
            try {
                final double longitude = Double.parseDouble(NumberUtils.stringNumber(longitudeString)) / 1000000.0;
                final double latitude = Double.parseDouble(NumberUtils.stringNumber(latitudeString)) / 1000000.0;
                Map<String, Object> chargeMap = chargeCarNotice(vid, longitude, latitude);
                if (MapUtils.isNotEmpty(chargeMap)) {
                    result.add(chargeMap);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            return result;
        } else {
            vidLowSocCount.remove(vid);

            final Map<String, Object> normalSocNotice = vidSocNotice.get(vid);
            if(null == normalSocNotice) {
                return null;
            }

            final int normalSocCount = vidNormSoc.getOrDefault(vid, 0) + 1;
            vidNormSoc.put(vid, normalSocCount);

            paramsRedisUtil.autoLog(vid, v->{
                logger.info("VID[{}]判定为SOC正常第[{}]次", v, normalSocCount);
            });

            // 0-初始化, 1-异常开始, 2-异常持续, 3-异常结束
            int status = (int)normalSocNotice.getOrDefault("status", 0);
            if (status != 1 && status != 2) {
                return null;
            }

            if(1 == normalSocCount) {
                normalSocNotice.put("etime", timeString);
                normalSocNotice.put("elocation", location);
                normalSocNotice.put("ethreshold", socAlarm);
                normalSocNotice.put("esoc", socNum);

                paramsRedisUtil.autoLog(vid, v->{
                    logger.info("VID[{}]SOC正常首帧初始化", v);
                });
            }

            if(normalSocCount < lowSocJudgeNum) {
                return null;
            }

            final Long firstNormalSocTime;
            try {
                firstNormalSocTime = timeformat.stringTimeLong(normalSocNotice.get("etime").toString());
            } catch (ParseException e) {
                e.printStackTrace();
                return result;
            }

            final Long nowTime = date.getTime();
            if (nowTime - firstNormalSocTime <= lowsocIntervalMillisecond) {
                return result;
            }

            normalSocNotice.put("status", 3);
            normalSocNotice.put("elazy", lowsocIntervalMillisecond);
            normalSocNotice.put("noticeTime", noticeTime);

            vidSocNotice.remove(vid);
            recorder.del(db, socRedisKeys, vid);

            result.add(normalSocNotice);

            paramsRedisUtil.autoLog(vid, v->{
                logger.info("VID[{}]SOC正常通知发送", v, normalSocNotice.get("msgId"));
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
        Map<String, FillChargeCar> fillvidgps = SysRealDataCache.chargeCars();
        Map<Double, FillChargeCar> chargeCarInfo = findNearFill(longitude, latitude, fillvidgps);

        if (null != chargeCarInfo) {
            Map<String, Object> chargeMap = new TreeMap<String, Object>();
            List<Map<String, Object>> chargeCars = new LinkedList<Map<String, Object>>();
            Map<String, String> topnCars = new TreeMap<String, String>();
            int cts = 0;
            for (Map.Entry<Double, FillChargeCar> entry : chargeCarInfo.entrySet()) {
                cts++;
                if (cts > topn) {
                    break;
                }
                double distance = entry.getKey();
                FillChargeCar chargeCar = entry.getValue();

                //save to redis map
                Map<String, Object> jsonMap = new TreeMap<String, Object>();
                jsonMap.put("vid", chargeCar.vid);
                jsonMap.put("LONGITUDE", chargeCar.longitude);
                jsonMap.put("LATITUDE", chargeCar.latitude);
                jsonMap.put("lastOnline", chargeCar.lastOnline);
                jsonMap.put("distance", distance);

                String jsonString = JSON.toJSONString(jsonMap);
                topnCars.put("" + cts, jsonString);
                //send to kafka map
                Map<String, Object> kMap = new TreeMap<String, Object>();
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
                chargeMap.put("msgType", "CHARGE_CAR_NOTICE");
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
//			String fillvid = entry.getKey();
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
     * 里程跳变处理
     *
     * @param dat
     * @return 实时里程跳变通知notice，treemap类型。
     */
    private Map<String, Object> mileHopHandle(Map<String, String> dat) {
        if (MapUtils.isEmpty(dat)) {
            return null;
        }
        try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
            String msgType = dat.get(SysDefine.MESSAGETYPE);
            if (StringUtils.isEmpty(vid)
                    || StringUtils.isEmpty(time)
                    || StringUtils.isEmpty(msgType)) {
                return null;
            }
            Map<String, Object> notice = null;
            if (CommandType.SUBMIT_REALTIME.equals(msgType)) {

                String mileage = dat.get(DataKey._2202_TOTAL_MILEAGE);//当前总里程

                if (vidLastDat.containsKey(vid)) {
                    //mileage如果是数字字符串则返回，不是则返回字符串“0”
                    mileage = NumberUtils.stringNumber(mileage);
                    if (!"0".equals(mileage)) {
                        int mile = Integer.parseInt(mileage);//当前总里程
                        Map<String, Object> lastMap = vidLastDat.get(vid);
                        int lastMile = (int) lastMap.get(DataKey._2202_TOTAL_MILEAGE);//上一帧的总里程
                        int nowmileHop = Math.abs(mile - lastMile);//里程跳变

                        if (nowmileHop >= mileHop) {
                            String lastTime = (String) lastMap.get(DataKey.TIME);//上一帧的时间即为跳变的开始时间
                            String vin = dat.get(DataKey.VEHICLE_NUMBER);
                            notice = new TreeMap<String, Object>();
                            notice.put("msgType", "HOP_MILE");//这些字段是前端方面要求的。
                            notice.put("vid", vid);
                            notice.put("vin", vin);
                            notice.put("stime", lastTime);
                            notice.put("etime", time);
                            notice.put("stmile", lastMile);
                            notice.put("edmile", mile);
                            notice.put("hopValue", nowmileHop);
                        }
                    }
                }
                //如果vidLastDat中没有缓存此vid，则把这一帧报文中的vid、time、mileage字段缓存到LastDat
                setLastDat(dat);
            }

            return notice;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void setLastDat(Map<String, String> dat) {
        String mileage = dat.get(DataKey._2202_TOTAL_MILEAGE);
        mileage = NumberUtils.stringNumber(mileage);
        if (!"0".equals(mileage)) {
            Map<String, Object> lastMap = new TreeMap<String, Object>();
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
            int mile = Integer.parseInt(mileage);
            lastMap.put(DataKey.VEHICLE_ID, vid);
            lastMap.put(DataKey.TIME, time);
            lastMap.put(DataKey._2202_TOTAL_MILEAGE, mile);
            vidLastDat.put(vid, lastMap);
        }
    }

    /**
     * IGNITE_SHUT_MESSAGE
     * 点火熄火
     *
     * @param dat
     * @return
     */
    private Map<String, Object> igniteOrShut(Map<String, String> dat) {
        if (MapUtils.isEmpty(dat)) {
            return null;
        }
        try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String vin = dat.get(DataKey.VEHICLE_NUMBER);
            String time = dat.get(DataKey.TIME);
            String carStatus = dat.get(DataKey._3201_CAR_STATUS);
            if (StringUtils.isEmpty(vid)
                    || StringUtils.isEmpty(time)
                    || StringUtils.isEmpty(carStatus)) {
                return null;
            }

            String latit = dat.get(DataKey._2503_LATITUDE);
            String longi = dat.get(DataKey._2502_LONGITUDE);
            String location = longi + "," + latit;
            String noticetime = timeformat.toDateString(new Date());
            String speed = dat.get(DataKey._2201_SPEED);
            String socStr = dat.get(DataKey._7615_STATE_OF_CHARGE);
            String mileageStr = dat.get(DataKey._2202_TOTAL_MILEAGE);

            double soc = -1;
            if (!StringUtils.isEmpty(socStr)) {
                soc = Double.parseDouble(NumberUtils.stringNumber(socStr));
                if (-1 != soc) {

                    lastSoc.put(vid, soc);
                }
            } else {
                if (lastSoc.containsKey(vid)) {
                    soc = lastSoc.get(vid);
                }
            }
            double mileage = -1;
            if (!StringUtils.isEmpty(mileageStr)) {
                mileage = Double.parseDouble(NumberUtils.stringNumber(mileageStr));
                if (-1 != mileage) {

                    lastMile.put(vid, mileage);
                }
            } else {
                if (lastMile.containsKey(vid)) {
                    mileage = lastMile.get(vid);
                }
            }

            double maxSpd = -1;
            if ("1".equals(carStatus)
                    || "2".equals(carStatus)) {
                double spd = -1;
                if (!igniteShutMaxSpeed.containsKey(vid)) {
                    igniteShutMaxSpeed.put(vid, maxSpd);
                }
                if (!StringUtils.isEmpty(speed)) {

                    maxSpd = igniteShutMaxSpeed.get(vid);
                    spd = Double.parseDouble(NumberUtils.stringNumber(speed));
                    if (spd > maxSpd) {
                        maxSpd = spd;
                        igniteShutMaxSpeed.put(vid, maxSpd);
                    }
                }

            }
            if ("1".equals(carStatus)) {//是否点火
                int cnts = 0;
                if (vidIgnite.containsKey(vid)) {
                    cnts = vidIgnite.get(vid);
                }
                cnts++;
                vidIgnite.put(vid, cnts);
                if (vidShut.containsKey(vid)) {
                    vidShut.remove(vid);
                }
                if (cnts >= 2) {

                    Map<String, Object> notice = vidIgniteShutNotice.get(vid);
                    if (null == notice) {
                        notice = new TreeMap<String, Object>();
                        notice.put("msgType", "IGNITE_SHUT_MESSAGE");
                        notice.put("vid", vid);
                        notice.put("vin", vin);
                        notice.put("msgId", UUID.randomUUID().toString());
                        notice.put("stime", time);
                        notice.put("soc", soc);
                        notice.put("ssoc", soc);
                        notice.put("mileage", mileage);
                        notice.put("status", 1);
                        notice.put("location", location);
                    } else {
                        double ssoc = (double) notice.get("ssoc");
                        double energy = Math.abs(ssoc - soc);
                        notice.put("soc", soc);
                        notice.put("mileage", mileage);
                        notice.put("maxSpeed", maxSpd);
                        notice.put("energy", energy);
                        notice.put("status", 2);
                        notice.put("location", location);
                    }
                    notice.put("noticetime", noticetime);
                    vidIgniteShutNotice.put(vid, notice);

                    if (1 == (int) notice.get("status")) {
                        return notice;
                    }
                }
            } else if ("2".equals(carStatus)) {//是否熄火
                if (vidIgnite.containsKey(vid)) {
                    int cnts = 0;
                    if (vidShut.containsKey(vid)) {
                        cnts = vidShut.get(vid);
                    }
                    cnts++;
                    vidShut.put(vid, cnts);

                    if (cnts >= 1) {
                        Map<String, Object> notice = vidIgniteShutNotice.get(vid);
                        vidIgnite.remove(vid);
                        vidIgniteShutNotice.remove(vid);

                        if (null != notice) {
                            double ssoc = (double) notice.get("ssoc");
                            double energy = Math.abs(ssoc - soc);
                            notice.put("soc", soc);
                            notice.put("mileage", mileage);
                            notice.put("maxSpeed", maxSpd);
                            notice.put("energy", energy);
                            notice.put("status", 3);
                            notice.put("location", location);
                            notice.put("etime", time);
                            notice.put("noticetime", noticetime);
                            vidShut.remove(vid);
                            return notice;
                        }
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * ???
     *
     * @param dat
     * @return
     */
    private Map<String, Object> flySe(Map<String, String> dat) {
        if (MapUtils.isEmpty(dat)) {
            return null;
        }
        try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
            String speed = dat.get(DataKey._2201_SPEED);
            String rev = dat.get(DataKey._2303_DRIVING_ELE_MAC_REV);
            if (StringUtils.isEmpty(vid)
                    || StringUtils.isEmpty(time)
                    || StringUtils.isEmpty(speed)
                    || StringUtils.isEmpty(rev)) {
                return null;
            }
            double spd = Double.parseDouble(NumberUtils.stringNumber(speed));
            double rv = Double.parseDouble(NumberUtils.stringNumber(rev));
            String latit = dat.get(DataKey._2503_LATITUDE);
            String longi = dat.get(DataKey._2502_LONGITUDE);
            String location = longi + "," + latit;
            String noticetime = timeformat.toDateString(new Date());
            if (spd > 0 && rv > 0) {

                int cnts = 0;
                if (vidFlySt.containsKey(vid)) {
                    cnts = vidFlySt.get(vid);
                }
                cnts++;
                vidFlySt.put(vid, cnts);
                if (cnts >= 10) {

                    Map<String, Object> notice = vidFlyNotice.get(vid);
                    if (null == notice) {
                        notice = new TreeMap<String, Object>();
                        notice.put("msgType", "FLY_RECORD");
                        notice.put("vid", vid);
                        notice.put("msgId", UUID.randomUUID().toString());
                        notice.put("stime", time);
                        notice.put("count", cnts);
                        notice.put("status", 1);
                        notice.put("location", location);
                    } else {
                        notice.put("count", cnts);
                        notice.put("status", 2);
                        notice.put("location", location);
                    }
                    notice.put("noticetime", noticetime);
                    vidFlyNotice.put(vid, notice);

                    if (1 == (int) notice.get("status")) {
                        return notice;
                    }
                }
            } else {
                if (vidFlySt.containsKey(vid)) {
                    int cnts = 0;
                    if (vidFlyEd.containsKey(vid)) {
                        cnts = vidFlyEd.get(vid);
                    }
                    cnts++;
                    vidFlyEd.put(vid, cnts);

                    if (cnts >= 10) {
                        Map<String, Object> notice = vidFlyNotice.get(vid);
                        vidFlySt.remove(vid);
                        vidFlyNotice.remove(vid);
                        if (null != notice) {
                            notice.put("status", 3);
                            notice.put("location", location);
                            notice.put("etime", time);
                            notice.put("noticetime", noticetime);
                            return notice;
                        }
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 北汽异常用车
     *
     * @param dat
     * @return
     */
    private Map<String, Object> abnormalCar(Map<String, String> dat) {
        if (MapUtils.isEmpty(dat)) {
            return null;
        }
        try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
            String speed = dat.get(DataKey._2201_SPEED);
            if (StringUtils.isEmpty(vid)
                    || StringUtils.isEmpty(time)
                    || StringUtils.isEmpty(speed)) {
                return null;
            }
            String latit = dat.get(DataKey._2503_LATITUDE);
            String longi = dat.get(DataKey._2502_LONGITUDE);
            String location = longi + "," + latit;
            String noticetime = timeformat.toDateString(new Date());
            long msgtime = timeformat.stringTimeLong(time);
            double spd = Double.parseDouble(NumberUtils.stringNumber(speed));
            if (spd > 0) {
                int cnts = 0;
                if (vidSpeedGtZero.containsKey(vid)) {
                    cnts = vidSpeedGtZero.get(vid);
                }
                cnts++;
                vidSpeedGtZero.put(vid, cnts);
                if (cnts >= 1) {

                    Map<String, Object> notice = vidSpeedGtZeroNotice.get(vid);
                    if (null == notice) {
                        notice = new TreeMap<String, Object>();
                        notice.put("msgType", "ABNORMAL_USE_VEH");
                        notice.put("vid", vid);
                        notice.put("msgId", UUID.randomUUID().toString());
                        notice.put("stime", time);
                        notice.put("count", cnts);
                        notice.put("status", 1);
                        notice.put("location", location);
                    } else {
                        long stime = timeformat.stringTimeLong((String) notice.get("stime"));
                        long timespace = msgtime - stime;
                        if ("true".equals(notice.get("isNotice"))) {
                            notice.put("count", cnts);
                            notice.put("status", 2);
                            notice.put("location", location);
                        } else {
                            if (timespace >= 150000) {
                                notice.put("utime", timespace / 1000);
                                notice.put("status", 1);
                                notice.put("isNotice", "true");
                                notice.put("noticetime", noticetime);
                                vidSpeedGtZeroNotice.put(vid, notice);
                                return notice;
                            }
                        }
                    }
                    notice.put("noticetime", noticetime);
                    vidSpeedGtZeroNotice.put(vid, notice);

//					if(1 == (int)notice.get("status"))
//						return notice;
                }
            } else {
                if (vidSpeedGtZero.containsKey(vid)) {
                    int cnts = 0;
                    if (vidSpeedZero.containsKey(vid)) {
                        cnts = vidSpeedZero.get(vid);
                    }
                    cnts++;
                    vidSpeedZero.put(vid, cnts);

                    if (cnts >= 10) {
                        Map<String, Object> notice = vidSpeedGtZeroNotice.get(vid);
                        vidSpeedGtZero.remove(vid);
                        vidSpeedGtZeroNotice.remove(vid);
                        if (null != notice) {
                            notice.put("status", 3);
                            notice.put("location", location);
                            notice.put("etime", time);
                            notice.put("noticetime", noticetime);
                            return notice;
                        }
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 未定位车辆_于心沼
     */
    Map<String, Object> noGps(Map<String, String> dat) {
        if (MapUtils.isEmpty(dat)) {
            return null;
        }
        try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
            if (StringUtils.isEmpty(vid)
                    || StringUtils.isEmpty(time)) {
                return null;
            }
            String longitude = dat.get(DataKey._2502_LONGITUDE);
            String latitude = dat.get(DataKey._2503_LATITUDE);
            //noticetime为当前时间
            Date date = new Date();
            String noticetime = timeformat.toDateString(date);

            boolean isValid = true;
            if (!StringUtils.isEmpty(latitude)
                    && !StringUtils.isEmpty(longitude)) {
                latitude = NumberUtils.stringNumber(latitude);
                longitude = NumberUtils.stringNumber(longitude);
                double latitd = Double.parseDouble(latitude);
                double longid = Double.parseDouble(longitude);
                if (latitd < 0 || latitd > 180000000
                        || longid < 0 || longid > 180000000) {
                    isValid = false;
                }
            }
            if (!isValid) {

                int cnts = 0;
                //vidnogps缓存无gps车辆的vid和报文帧数
                if (vidNoGps.containsKey(vid)) {
                    cnts = vidNoGps.get(vid);
                }
                cnts++;
                vidNoGps.put(vid, cnts);
                if (cnts >= nogpsJudgeNum) {
                    //vidgpsNotice缓存通知，第一次发通知
                    Map<String, Object> notice = vidgpsNotice.get(vid);
                    if (null == notice) {
                        notice = new TreeMap<String, Object>();
                        notice.put("msgType", "NO_POSITION_VEH");
                        notice.put("vid", vid);
                        notice.put("msgId", UUID.randomUUID().toString());
                        notice.put("stime", time);
                        notice.put("count", cnts);
                        notice.put("status", 1);//1开始，2持续，3结束

                    } else {
                        //不是第一次了
                        notice.put("count", cnts);
                    }
                    notice.put("noticetime", noticetime);
                    vidgpsNotice.put(vid, notice);

                    Long nowTime = date.getTime();
                    Long firstNogpsTime = timeformat.stringTimeLong(notice.get("stime").toString());
                    //如果满足条件，将judgeIsSend中的GpsIsSend置为true，意思是已经发送了未定位通知。
                    if (1 == (int) notice.get("status") && nowTime - firstNogpsTime > nogpsIntervalTime) {
                        IsSendNoticeCache judgeIsSendNotice = vidIsSendNoticeCache.get(vid);
                        if (null == judgeIsSendNotice) {
                            judgeIsSendNotice = new IsSendNoticeCache();
                            vidIsSendNoticeCache.put(vid, judgeIsSendNotice);
                        }
                        judgeIsSendNotice.gpsIsSend = true;
                        return notice;

                    }
                }
            } else {
                if (vidNoGps.containsKey(vid)) {//车的GPS是有效的，并且vidnogps中包含这辆车，才有可能发送结束通知报文
                    int cnts = 0;
                    if (vidNormalGps.containsKey(vid)) {
                        cnts = vidNormalGps.get(vid);
                    }
                    cnts++;
                    vidNormalGps.put(vid, cnts);
                    //有效gps报文超过hasgpsJudgeNum  || 有效gps报文在（3贞以上，10贞以下）同时车辆登出
                    if (cnts >= hasgpsJudgeNum || (cnts > 3 && null != dat.get(SUBMIT_LOGIN.LOGOUT_TIME))) {

                        vidNormalGps.remove(vid);
                        IsSendNoticeCache cache=vidIsSendNoticeCache.get(vid);
                        //如果未定位开始通知没有发送，则不会发送结束通知，只会把各个缓存清空
                        if (null != cache && !cache.gpsIsSend) {
                            vidNoGps.remove(vid);
                            vidgpsNotice.remove(vid);
                            return null;
                        }

                        if(null == cache){
                            cache = new IsSendNoticeCache();
                            vidIsSendNoticeCache.put(vid,cache);
                        }

                        Map<String, Object> notice = vidgpsNotice.get(vid);
                        vidNoGps.remove(vid);
                        vidgpsNotice.remove(vid);
                        cache.gpsIsSend = false;
                        if (null != notice) {
                            String location = longitude + "," + latitude;
                            notice.put("status", 3);
                            notice.put("location", location);
                            notice.put("etime", time);
                            notice.put("noticetime", noticetime);
                            return notice;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private Map<String, Object> onOffline(Map<String, String> dat) {
        if (MapUtils.isEmpty(dat)) {
            return null;
        }
        try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String vin = dat.get(DataKey.VEHICLE_NUMBER);
            String time = dat.get(DataKey.TIME);
            if (StringUtils.isEmpty(vid)
                    || StringUtils.isEmpty(time)
                    || StringUtils.isEmpty(vin)) {
                return null;
            }

            boolean isoff = isOffline(dat);
            if (!isoff) {//上线
                Map<String, Object> notice = vidOnOffNotice.get(vid);
                if (null == notice) {
                    String noticetime = timeformat.toDateString(new Date());
                    notice = new TreeMap<String, Object>();
                    notice.put("msgType", "ON_OFF");
                    notice.put("vid", vid);
                    notice.put("vin", vin);
                    notice.put("msgId", UUID.randomUUID().toString());
                    notice.put("stime", time);
                    notice.put("status", 1);
                    notice.put("noticetime", noticetime);
                } else {
                    notice.put("status", 2);
                }
                vidOnOffNotice.put(vid, notice);

                if (1 == (int) notice.get("status")) {
                    recorder.save(db, onOffRedisKeys, vid, notice);
                    return notice;
                }
            } else {//是否离线
                if (vidOnOffNotice.containsKey(vid)) {

                    Map<String, Object> notice = vidOnOffNotice.get(vid);
                    if (null != notice) {
                        String noticetime = timeformat.toDateString(new Date());
                        notice.put("status", 3);
                        notice.put("etime", time);
                        notice.put("noticetime", noticetime);
                        vidOnOffNotice.remove(vid);
                        recorder.del(db, onOffRedisKeys, vid);
                        return notice;
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean isOffline(Map<String, String> dat) {
        String msgType = dat.get(SysDefine.MESSAGETYPE);
        if (CommandType.SUBMIT_LOGIN.equals(msgType)) {
            String type = dat.get(ProtocolItem.REG_TYPE);
            if ("1".equals(type)) {
                return false;
            } else if ("2".equals(type)) {
                return true;
            } else {
                String logoutTime = dat.get(SUBMIT_LOGIN.LOGOUT_TIME);
                String loginTime = dat.get(SUBMIT_LOGIN.LOGIN_TIME);
                if (!StringUtils.isEmpty(logoutTime)
                        && !StringUtils.isEmpty(logoutTime)) {
                    long logout = Long.parseLong(NumberUtils.stringNumber(logoutTime));
                    long login = Long.parseLong(NumberUtils.stringNumber(loginTime));
                    if (login > logout) {
                        return false;
                    }
                    return true;

                } else {
                    if (StringUtils.isEmpty(loginTime)) {
                        return false;
                    }
                    return true;
                }
            }
        } else if (CommandType.SUBMIT_LINKSTATUS.equals(msgType)) {
            String linkType = dat.get(SUBMIT_LINKSTATUS.LINK_TYPE);
            if ("1".equals(linkType)
                    || "2".equals(linkType)) {
                return false;
            } else if ("3".equals(linkType)) {
                return true;
            }
        } else if (CommandType.SUBMIT_REALTIME.equals(msgType)) {
            return false;
        }
        return false;
    }

    /**
     * 检查所有车辆是否离线，离线则发送离线通知
     *
     * @param now
     * @return
     */
    public List<Map<String, Object>> offlineMethod(long now) {
        if (null == lastTime || lastTime.size() == 0) {
            return null;
        }
        List<Map<String, Object>> notices = new LinkedList<Map<String, Object>>();
        String noticetime = timeformat.toDateString(new Date(now));
        List<String> needRemoves = new LinkedList<String>();
        for (Map.Entry<String, Long> entry : lastTime.entrySet()) {
            long last = entry.getValue();
            if (now - last > offlinetime) {
                String vid = entry.getKey();
                needRemoves.add(vid);
                Map<String, Object> msg = vidOnOffNotice.get(vid);//vidOnOffNotice是车辆上线通知缓存
                //如果msg不为null，说明之前在线，现在离线，需要发送离线通知
                if (null != msg) {
                    getOffline(msg, noticetime);
                    if (null != msg) {
                        notices.add(msg);
                    }
                }
            }
        }
        for (String vid : needRemoves) {
            lastTime.remove(vid);
            vidOnOffNotice.remove(vid);
        }
        if (notices.size() > 0) {
            return notices;
        }
        return null;
    }

    public List<Map<String, Object>> offlineMethod2(long now) {
        if (null == lastTime || lastTime.size() == 0) {
            return null;
        }
        List<Map<String, Object>> notices = new LinkedList<Map<String, Object>>();
        String noticetime = timeformat.toDateString(new Date(now));
        List<String> needRemoves = new LinkedList<String>();
        for (Map.Entry<String, Long> entry : lastTime.entrySet()) {
            long last = entry.getValue();
            if (now - last > offlinetime) {
                String vid = entry.getKey();
                needRemoves.add(vid);
                Map<String, Object> msg = vidFlyNotice.get(vid);
                if (null != msg) {

                    getOffline(msg, noticetime);
                    if (null != msg) {
                        notices.add(msg);
                    }
                }
                msg = vidSocNotice.get(vid);
                if (null != msg) {

                    getOffline(msg, noticetime);
                    if (null != msg) {
                        notices.add(msg);
                    }
                }
                msg = vidcanNotice.get(vid);
                if (null != msg) {

                    getOffline(msg, noticetime);
                    if (null != msg) {
                        notices.add(msg);
                    }
                }
                msg = vidIgniteShutNotice.get(vid);
                if (null != msg) {

                    getOffline(msg, noticetime);
                    if (null != msg) {
                        notices.add(msg);
                    }
                }
                msg = vidgpsNotice.get(vid);
                if (null != msg) {

                    getOffline(msg, noticetime);
                    if (null != msg) {
                        notices.add(msg);
                    }
                }
                msg = vidSpeedGtZeroNotice.get(vid);
                if (null != msg) {

                    getOffline(msg, noticetime);
                    if (null != msg) {
                        notices.add(msg);
                    }
                }
                msg = vidOnOffNotice.get(vid);
                if (null != msg) {

                    getOffline(msg, noticetime);
                    if (null != msg) {
                        notices.add(msg);
                    }
                }
            }
        }

        for (String vid : needRemoves) {
            lastTime.remove(vid);
            vidFlyNotice.remove(vid);
            vidSocNotice.remove(vid);
            vidcanNotice.remove(vid);
            vidgpsNotice.remove(vid);
            vidSpeedGtZeroNotice.remove(vid);
            vidIgniteShutNotice.remove(vid);
            vidOnOffNotice.remove(vid);

            vidFlyEd.remove(vid);
            vidFlySt.remove(vid);
            vidShut.remove(vid);
            vidIgnite.remove(vid);
            vidSpeedGtZero.remove(vid);
            vidSpeedZero.remove(vid);
            vidLowSocCount.remove(vid);
            vidNormSoc.remove(vid);
            vidNoCanCount.remove(vid);
            vidNormalCanCount.remove(vid);
            vidNoGps.remove(vid);
            vidNormalGps.remove(vid);

        }
        if (notices.size() > 0) {
            return notices;
        }


        return null;
    }

    public void getOffline(Map<String, Object> msg, String noticetime) {
        if (null != msg) {
            msg.put("noticetime", noticetime);
            msg.put("status", 3);
            msg.put("etime", noticetime);
        }
    }

    void restartInit(boolean isRestart) {
        if (isRestart) {
            recorder.rebootInit(db, onOffRedisKeys, vidOnOffNotice);
            recorder.rebootInit(db, socRedisKeys, vidSocNotice);
            recorder.rebootInit(CarLockStatusChangeJudge.db, CarLockStatusChangeJudge.lockStatusRedisKeys, vidLockStatus);
        }
    }



    public static void main(String[] args) {
		/*Map<Double, String> carSortMap = new TreeMap<Double, String>();
		carSortMap.put(256.3, "tttttt");
		carSortMap.put(122.3, "tttttt");
		carSortMap.put(356.3, "tttttt");
		carSortMap.put(299.3, "tttttt");
		carSortMap.put(156.3, "tttttt");
		carSortMap.put(756.3, "tttttt");
		
		for (Map.Entry<Double, String> entry : carSortMap.entrySet()) {
			
			System.out.println(entry.getKey()+":"+entry.getValue());
		}*/
        String la = null;
        String lt = null;
        System.out.println(la + "," + lt);
    }

}
