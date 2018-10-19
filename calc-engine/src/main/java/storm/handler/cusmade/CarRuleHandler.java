package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.SysRealDataCache;
import storm.cache.VehicleCache;
import storm.constant.FormatConstant;
import storm.dao.DataToRedis;
import storm.dto.FillChargeCar;
import storm.handler.ctx.Recorder;
import storm.handler.ctx.RedisRecorder;
import storm.protocol.CommandType;
import storm.protocol.SUBMIT_LINKSTATUS;
import storm.protocol.SUBMIT_LOGIN;
import storm.system.*;
import storm.util.*;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutionException;

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

    private static final Logger LOG = LoggerFactory.getLogger(CarRuleHandler.class);
    private static final ConfigUtils CONFIG_UTILS = ConfigUtils.getInstance();
    private static final ParamsRedisUtil PARAMS_REDIS_UTIL = ParamsRedisUtil.getInstance();
    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();
    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();

    

    private final Map<String, Integer> vidGpsFaultCount = new HashMap<>();
    private final Map<String, Integer> vidGpsNormalCount = new HashMap<>();
    private static int gpsFaultFrameTriggerCount = 5;
    private static int gpsNormalFrameTriggerCount = 10;
    private static long gpsFaultIntervalMillisecond = 60000L;
    private static long gpsNormalIntervalMillisecond = 60000L;

    /**
     * 无CAN计数器
     */
    private final Map<String, Integer> vidNoCanCount = new HashMap<>();
    /**
     * CAN正常计数器
     */
    private final Map<String, Integer> vidNormalCanCount = new HashMap<>();

    private Map<String, Integer> vidIgnite = new HashMap<>();
    private Map<String, Integer> vidShut = new HashMap<>();
    private Map<String, Double> igniteShutMaxSpeed = new HashMap<>();
    private Map<String, Double> lastSoc = new HashMap<>();
    private Map<String, Double> lastMile = new HashMap<>();


    private Map<String, Integer> vidSpeedGtZero = new HashMap<>();
    private Map<String, Integer> vidSpeedZero = new HashMap<>();

    private Map<String, Integer> vidFlySt = new HashMap<>();
    private Map<String, Integer> vidFlyEd = new HashMap<>();
    private Map<String, Map<String, Object>> vidcanNotice = new HashMap<>();
    private Map<String, Map<String, Object>> vidIgniteShutNotice = new HashMap<>();
    private Map<String, Map<String, String>> vidGpsNotice = new HashMap<>();
    private Map<String, Map<String, Object>> vidSpeedGtZeroNotice = new HashMap<>();
    private Map<String, Map<String, Object>> vidFlyNotice = new HashMap<>();
    private Map<String, Map<String, Object>> vidOnOffNotice = new HashMap<>();

    private Map<String, Map<String, Object>> vidLastDat = new HashMap<>();//vid和最后一帧数据的缓存
    private Map<String, Long> lastTime = new HashMap<>();


    DataToRedis redis;
    private Recorder recorder;
    static String onOffRedisKeys = "vehCache.qy.onoff.notice";

    static int topn = 20;
    // 600秒
    static long offlinetime = 600000;
    // 5次
    static int nocanJudgeNum = 5;
    // 10次
    static int hascanJudgeNum = 10;
    // 2公里 ，单位是0.1km
    static int mileHop = 20;
    // 600秒
    static long nocanIntervalTime = 600000L;

    static int db = 6;
    // 1代表规则启用，0代表规则关闭
    static int socRule = 0;
    static int enableCanRule = 0;
    static int igniteRule = 0;
    static int gpsRule = 0;
    static int abnormalRule = 0;
    static int flyRule = 0;
    static int onoffRule = 0;
    static int mileHopRule = 0;
    static int carLockStatueChangeJudgeRule = 0;

    private final CarNoCanJudge carNoCanJudge = new CarNoCanJudge();

    private static final CarLowSocJudge carLowSocJudge = new CarLowSocJudge();

    private final CarLockStatusChangeJudge carLockStatusChangeJudge = new CarLockStatusChangeJudge();

    //以下参数可以通过读取配置文件进行重置
    static {
        LOG.warn("运行静态代码块，读取配置文件中值");
        if (null != CONFIG_UTILS.sysDefine) {
            LOG.warn("运行静态代码块，判断配置文件是否存在");
            String off = CONFIG_UTILS.sysDefine.getProperty(StormConfigKey.REDIS_OFFLINE_SECOND);
            if (!StringUtils.isEmpty(off)) {
                offlinetime = Long.parseLong(off) * 1000;
            }

            String value = CONFIG_UTILS.sysDefine.getProperty("sys.carlockstatus.rule");
            if (!StringUtils.isEmpty(value)) {
                carLockStatueChangeJudgeRule = Integer.parseInt(value);
                value = null;
            }

            value = CONFIG_UTILS.sysDefine.getProperty("sys.soc.rule");
            if (!StringUtils.isEmpty(value)) {
                socRule = Integer.parseInt(value);
                value = null;
            }

            value = CONFIG_UTILS.sysDefine.getProperty("sys.can.rule");
            if (!StringUtils.isEmpty(value)) {
                enableCanRule = Integer.parseInt(value);
                value = null;
            }

            value = CONFIG_UTILS.sysDefine.getProperty("sys.ignite.rule");
            if (!StringUtils.isEmpty(value)) {
                igniteRule = Integer.parseInt(value);
                value = null;
            }

            value = CONFIG_UTILS.sysDefine.getProperty("sys.gps.rule");
            if (!StringUtils.isEmpty(value)) {
                gpsRule = Integer.parseInt(value);
                value = null;
            }

            value = CONFIG_UTILS.sysDefine.getProperty("sys.abnormal.rule");
            if (!StringUtils.isEmpty(value)) {
                abnormalRule = Integer.parseInt(value);
                value = null;
            }

            value = CONFIG_UTILS.sysDefine.getProperty("sys.fly.rule");
            if (!StringUtils.isEmpty(value)) {
                flyRule = Integer.parseInt(value);
                value = null;
            }

            value = CONFIG_UTILS.sysDefine.getProperty("sys.onoff.rule");
            if (!StringUtils.isEmpty(value)) {
                onoffRule = Integer.parseInt(value);
                value = null;
            }

            value = CONFIG_UTILS.sysDefine.getProperty("sys.milehop.rule");
            if (!StringUtils.isEmpty(value)) {
                mileHopRule = Integer.parseInt(value);
                value = null;
            }
//<<................................................从配置文件中读取SOC相关阈值..........................................>>
            value = CONFIG_UTILS.sysDefine.getProperty(SysDefine.SOC_FAULT_JUDGE_TIME);
            if (!StringUtils.isEmpty(value)) {
                carLowSocJudge.setLowSocFaultIntervalMillisecond(Long.parseLong(value));
                value = null;
            }
            value = CONFIG_UTILS.sysDefine.getProperty(SysDefine.SOC_NORMAL_JUDGE_TIME);
            if (!StringUtils.isEmpty(value)) {
                carLowSocJudge.setLowSocNormalIntervalMillisecond(Long.parseLong(value));
                value = null;
            }
            value = CONFIG_UTILS.sysDefine.getProperty(SysDefine.SOC_FAULT_JUDGE_NO);
            if (!StringUtils.isEmpty(value)) {
                carLowSocJudge.setLowSocFaultJudgeNum(Integer.parseInt(value));
                value = null;
            }
            value = CONFIG_UTILS.sysDefine.getProperty(SysDefine.SOC_NORMAL_JUDGE_NO);
            if (!StringUtils.isEmpty(value)) {
                carLowSocJudge.setLowSocNormalJudgeNum(Integer.parseInt(value));
                value = null;
            }

            value = CONFIG_UTILS.sysDefine.getProperty(SysDefine.LT_ALARM_SOC_START);
            if (!StringUtils.isEmpty(value)) {
                carLowSocJudge.setSocLowAlarm_StartThreshold(Integer.parseInt(value));
                value = null;
            }
            value = CONFIG_UTILS.sysDefine.getProperty(SysDefine.LT_ALARM_SOC_END);
            if (!StringUtils.isEmpty(value)) {
                carLowSocJudge.setLowSocAlarm_EndThreshold(Integer.parseInt(value));
                value = null;
            }


            value = CONFIG_UTILS.sysDefine.getProperty(SysDefine.GPS_NOVALUE_CONTINUE_NO);
            if (NumberUtils.isDigits(value)) {
                gpsFaultFrameTriggerCount = Integer.parseInt(value);
                value = null;
            }

            value = CONFIG_UTILS.sysDefine.getProperty(SysDefine.GPS_HASVALUE_CONTINUE_NO);
            if (NumberUtils.isDigits(value)) {
                gpsNormalFrameTriggerCount = Integer.parseInt(value);
                value = null;
            }

            value = CONFIG_UTILS.sysDefine.getProperty(SysDefine.GPS_JUDGE_TIME);
            if (!StringUtils.isEmpty(value)) {
                gpsFaultIntervalMillisecond = Long.parseLong(value);
                gpsNormalIntervalMillisecond = gpsFaultIntervalMillisecond;
                value = null;
            }
        }
        init();
    }

    //以下参数可以通过读取redis定时进行重新加载
    static void init() {
        //<<...................................从redis中定时读取SOC相关阈值..................................>>
        Object socVal_Start = PARAMS_REDIS_UTIL.PARAMS.get(SysDefine.LT_ALARM_SOC_START);
        if (null != socVal_Start) {
            carLowSocJudge.setSocLowAlarm_StartThreshold((int) socVal_Start);
        }
        Object socVal_End = PARAMS_REDIS_UTIL.PARAMS.get(SysDefine.LT_ALARM_SOC_END);
        if (null != socVal_End) {
            carLowSocJudge.setSocLowAlarm_StartThreshold((int) socVal_End);
        }

        // soc过低开始的帧数和时间阈值，soc正常的开始帧数和时间阈值
        Object lowSocFaultJudgeCount = PARAMS_REDIS_UTIL.PARAMS.get(SysDefine.SOC_FAULT_JUDGE_NO);
        if (null != lowSocFaultJudgeCount) {
            carLowSocJudge.setLowSocFaultJudgeNum((int) lowSocFaultJudgeCount);
        }
        Object lowSocNormalJudgeCount = PARAMS_REDIS_UTIL.PARAMS.get(SysDefine.SOC_NORMAL_JUDGE_NO);
        if (null != lowSocNormalJudgeCount) {
            carLowSocJudge.setLowSocNormalJudgeNum((int) lowSocNormalJudgeCount);
        }
        Object lowSocFaultJudgeTime = PARAMS_REDIS_UTIL.PARAMS.get(SysDefine.SOC_FAULT_JUDGE_TIME);
        if (null != lowSocFaultJudgeTime) {
            carLowSocJudge.setLowSocFaultIntervalMillisecond(((int) lowSocFaultJudgeTime)*1L);
        }
        Object lowSocNormalJudgeTime = PARAMS_REDIS_UTIL.PARAMS.get(SysDefine.SOC_NORMAL_JUDGE_TIME);
        if (null != lowSocNormalJudgeTime) {
            carLowSocJudge.setLowSocNormalIntervalMillisecond(((int) lowSocNormalJudgeTime)*1L);
        }



        Object nocanJugyObj = PARAMS_REDIS_UTIL.PARAMS.get("can.novalue.continue.no");
        if (null != nocanJugyObj) {
            nocanJudgeNum = (int) nocanJugyObj;
        }
        Object hascanJugyObj = PARAMS_REDIS_UTIL.PARAMS.get("can.novalue.continue.no");
        if (null != hascanJugyObj) {
            hascanJudgeNum = (int) hascanJugyObj;
        }


        Object nogpsNum = PARAMS_REDIS_UTIL.PARAMS.get("gps.novalue.continue.no");
        if (null != nogpsNum) {
            gpsFaultFrameTriggerCount = (int) nogpsNum;
        }
        Object hasgpsNum = PARAMS_REDIS_UTIL.PARAMS.get("gps.novalue.continue.no");
        if (null != hasgpsNum) {
            gpsNormalFrameTriggerCount = (int) hasgpsNum;
        }
        Object hopnum = PARAMS_REDIS_UTIL.PARAMS.get("mile.hop.num");
        if (null != hopnum) {
            mileHop = ((int) hopnum) * 10;
        }
        Object nogpsJudgeTime = PARAMS_REDIS_UTIL.PARAMS.get(SysDefine.GPS_JUDGE_TIME);
        if (null != nogpsJudgeTime) {
            gpsFaultIntervalMillisecond = ((int) nogpsJudgeTime) * 1000L;
            gpsNormalIntervalMillisecond = gpsFaultIntervalMillisecond;
        }
        Object nocanJudgeTime = PARAMS_REDIS_UTIL.PARAMS.get("can.judge.time");
        if (null != nocanJudgeTime) {
            nocanIntervalTime = ((int) nocanJudgeTime) * 1000L;
        }

        {
            final String ruleOverride = PARAMS_REDIS_UTIL.PARAMS.getOrDefault(
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
                final String alarmCanFaultTriggerContinueCountString = PARAMS_REDIS_UTIL.PARAMS.get(
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
                final String alarmCanFaultTriggerTimeoutMillisecondString = PARAMS_REDIS_UTIL.PARAMS.get(
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
                final String alarmCanNormalTriggerContinueCountString = PARAMS_REDIS_UTIL.PARAMS.get(
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
                final String alarmCanNormalTriggerTimeoutMillisecondString = PARAMS_REDIS_UTIL.PARAMS.get(
                    SysDefine.NOTICE_CAN_NORMAL_TRIGGER_TIMEOUT_MILLISECOND).toString();
                final long alarmCanNormalTriggerTimeoutMillisecond = Long.parseLong(alarmCanNormalTriggerTimeoutMillisecondString);
                if(alarmCanNormalTriggerTimeoutMillisecond >= 0) {
                    CarNoCanJudge.setNormalTriggerTimeoutMillisecond(alarmCanNormalTriggerTimeoutMillisecond);
                }
            } catch (Exception ignored) {

            }
        }
    }

    {
        redis = new DataToRedis();
        recorder = new RedisRecorder(redis);
        restartInit(true);
    }

    public static void rebulid() {
        PARAMS_REDIS_UTIL.rebulid();
        init();
    }

    /**
     * 生成通知
     *
     * @param immutableMap
     * @return
     */
    @NotNull
    public List<Map<String, Object>> generateNotices(@NotNull final ImmutableMap<String, String> immutableMap) {

        // 为下面的方法做准备，生成相应的容器。
        final List<Map<String, Object>> list = new LinkedList<>();

        // 验证data的有效性
        if (MapUtils.isEmpty(immutableMap)
                || !immutableMap.containsKey(DataKey.VEHICLE_ID)
                || !immutableMap.containsKey(DataKey.TIME)) {
            return list;
        }

        final Map<String, String> data = Maps.newHashMap(immutableMap);

        String vid = data.get(DataKey.VEHICLE_ID);
        if (StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(data.get(DataKey.TIME))) {
            return list;
        }

        lastTime.put(vid, System.currentTimeMillis());

        if(PARAMS_REDIS_UTIL.isTraceVehicleId(vid)) {
            LOG.trace("VID[" + vid + "]进入车辆规则处理");
        }

        Map<String, Object> canJudge = null;
        Map<String, Object> igniteJudge = null;
        Map<String, Object> gpsJudge = null;
        Map<String, Object> abnormalJudge = null;
        Map<String, Object> flyJudge = null;
        Map<String, Object> onOffJudge = null;
        Map<String, Object> mileHopJudge = null;
        //3、如果规则启用了，则把dat放到相应的处理方法中。将返回结果放到list中，返回。

        if (1 == carLockStatueChangeJudgeRule) {
            final Map<String, Object> lockStatueChange = carLockStatusChangeJudge.carLockStatueChangeJudge(data);
            if (!MapUtils.isEmpty(lockStatueChange)) {
                list.add(lockStatueChange);
            }
        }

        if (1 == socRule) {
            //lowsoc(data)返回一个map，里面有vid和通知消息（treeMap）
            // SOC
            List<Map<String, Object>> socJudges = carLowSocJudge.processFrame(data);
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
            // 未定位车辆
            gpsJudge = new TreeMap<>();
            final Map<String, String> notice = noGps(data);
            if(MapUtils.isNotEmpty(notice)) {
                gpsJudge.putAll(notice);
            }
        }
        if (1 == abnormalRule) {
            // 异常用车
            abnormalJudge = abnormalCar(data);
        }
        if (1 == flyRule) {
            // 飞机功能（一般不用）
            flyJudge = flySe(data);
        }
        if (1 == onoffRule) {
            // 车辆上下线
            onOffJudge = onOffline(data);
        }
        if (1 == mileHopRule) {
            // 里程跳变处理
            mileHopJudge = mileHopHandle(data);
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
            String msgType = dat.get(DataKey.MESSAGE_TYPE);
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
                    mileage = org.apache.commons.lang.math.NumberUtils.isNumber(mileage) ? mileage : "0";
                    if (!"0".equals(mileage)) {
                        int mile = Integer.parseInt(mileage);//当前总里程
                        Map<String, Object> lastMap = vidLastDat.get(vid);
                        int lastMile = (int) lastMap.get(DataKey._2202_TOTAL_MILEAGE);//上一帧的总里程
                        int nowmileHop = Math.abs(mile - lastMile);//里程跳变

                        if (nowmileHop >= mileHop) {
                            String lastTime = (String) lastMap.get(DataKey.TIME);//上一帧的时间即为跳变的开始时间
                            String vin = dat.get(DataKey.VEHICLE_NUMBER);
                            notice = new TreeMap<>();
                            notice.put("msgType", NoticeType.HOP_MILE);//这些字段是前端方面要求的。
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
        mileage = org.apache.commons.lang.math.NumberUtils.isNumber(mileage) ? mileage : "0";
        if (!"0".equals(mileage)) {
            Map<String, Object> lastMap = new TreeMap<>();
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
            String noticetime = DateFormatUtils.format(new Date(), FormatConstant.DATE_FORMAT);
            String speed = dat.get(DataKey._2201_SPEED);
            String socStr = dat.get(DataKey._7615_STATE_OF_CHARGE);
            String mileageStr = dat.get(DataKey._2202_TOTAL_MILEAGE);

            double soc = -1;
            if (!StringUtils.isEmpty(socStr)) {
                soc = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(socStr) ? socStr : "0");
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
                mileage = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(mileageStr) ? mileageStr : "0");
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
                    spd = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(speed) ? speed : "0");
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
                        notice = new TreeMap<>();
                        notice.put("msgType", NoticeType.IGNITE_SHUT_MESSAGE);
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
            double spd = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(speed) ? speed : "0");
            double rv = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(rev) ? rev : "0");
            String latit = dat.get(DataKey._2503_LATITUDE);
            String longi = dat.get(DataKey._2502_LONGITUDE);
            String location = longi + "," + latit;
            String noticetime = DateFormatUtils.format(new Date(), FormatConstant.DATE_FORMAT);
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
                        notice = new TreeMap<>();
                        notice.put("msgType", NoticeType.FLY_RECORD);
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
            String noticetime = DateFormatUtils.format(new Date(), FormatConstant.DATE_FORMAT);
            long msgtime = DateUtils.parseDate(time, new String[]{FormatConstant.DATE_FORMAT}).getTime();
            double spd = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(speed) ? speed : "0");
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
                        notice = new TreeMap<>();
                        notice.put("msgType", NoticeType.ABNORMAL_USE_VEH);
                        notice.put("vid", vid);
                        notice.put("msgId", UUID.randomUUID().toString());
                        notice.put("stime", time);
                        notice.put("count", cnts);
                        notice.put("status", 1);
                        notice.put("location", location);
                    } else {
                        long stime = DateUtils.parseDate((String) notice.get("stime"), new String[]{FormatConstant.DATE_FORMAT}).getTime();
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

//                    if(1 == (int)notice.get("status"))
//                        return notice;
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
    Map<String, String> noGps(Map<String, String> dat) {

        if (MapUtils.isEmpty(dat)) {
            return null;
        }

        try {

            final String vid = dat.get(DataKey.VEHICLE_ID);
            final String timeString = dat.get(DataKey.TIME);
            if (StringUtils.isEmpty(vid)
                    || StringUtils.isEmpty(timeString)) {
                return null;
            }

            final long currentTimeMillis = System.currentTimeMillis();
            final String noticeTime = DateFormatUtils.format(currentTimeMillis, FormatConstant.DATE_FORMAT);

            final String orientationString= dat.get(DataKey._2501_ORIENTATION);
            final String longitudeString = dat.get(DataKey._2502_LONGITUDE);
            final String latitudeString = dat.get(DataKey._2503_LATITUDE);

            boolean isFault = isGpsFault(vid, orientationString, longitudeString, latitudeString);
            if (isFault) {
                // region 定位异常逻辑

                vidGpsNormalCount.remove(vid);

                final int gpsFaultCount = vidGpsFaultCount.getOrDefault(vid, 0) + 1;
                vidGpsFaultCount.put(vid, gpsFaultCount);

                PARAMS_REDIS_UTIL.autoLog(
                    vid,
                    ()-> LOG.info(
                        "VID[{}]判定为GPS故障第[{}]次",
                        vid,
                        gpsFaultCount));

                final Map<String, String> gpsFaultNotice = vidGpsNotice.getOrDefault(vid, new TreeMap<>());
                if (MapUtils.isEmpty(gpsFaultNotice)) {
                    gpsFaultNotice.put("msgType", NoticeType.NO_POSITION_VEH);
                    gpsFaultNotice.put("msgId", UUID.randomUUID().toString());
                    gpsFaultNotice.put("vid", vid);
                    gpsFaultNotice.put("status", "0");
                    vidGpsNotice.put(vid, gpsFaultNotice);

                    PARAMS_REDIS_UTIL.autoLog(
                        vid,
                        ()-> LOG.info(
                            "VID[{}]GPS故障首帧缓存初始化",
                            vid));

                }

                // 0-初始化, 1-异常开始, 2-异常持续, 3-异常结束
                String status = gpsFaultNotice.getOrDefault("status", "0");
                if (!"0".equals(status) && !"3".equals(status)) {

                    PARAMS_REDIS_UTIL.autoLog(
                        vid,
                        () -> LOG.info(
                            "VID[{}][{}]GPS故障不是初始化或已结束状态",
                            vid,
                            status
                        )
                    );
                    return null;
                }

                if (1 == gpsFaultCount) {

                    gpsFaultNotice.put("stime", timeString);

                    PARAMS_REDIS_UTIL.autoLog(
                        vid,
                        ()-> LOG.info(
                            "VID[{}]GPS故障首帧更新",
                            vid));
                }

                if (!gpsFaultNotice.containsKey("slocation")) {

                    try {
                        final String locationFromCache = getUsefulLocationFromCache(vid);
                        gpsFaultNotice.put("slocation", locationFromCache);
                        // 兼容性暂留
                        gpsFaultNotice.put("location", locationFromCache);
                    } catch (ExecutionException e) {
                        LOG.warn("获取定位缓存异常", e);
                    }
                }

                if(gpsFaultCount < gpsFaultFrameTriggerCount) {

                    PARAMS_REDIS_UTIL.autoLog(
                        vid,
                        ()-> LOG.info(
                            "VID[{}]GPS故障连续帧数不足[{}]帧",
                            vid,
                            gpsFaultFrameTriggerCount));
                    return null;
                }

                final long firstGpsFaultTime;
                try {
                    firstGpsFaultTime = DateUtils
                        .parseDate(
                            gpsFaultNotice.get("stime").toString(),
                            new String[]{FormatConstant.DATE_FORMAT})
                        .getTime();
                } catch (ParseException e) {
                    LOG.warn("解析开始时间异常", e);
                    gpsFaultNotice.put("stime", timeString);
                    return null;
                }

                if (currentTimeMillis - firstGpsFaultTime <= gpsFaultIntervalMillisecond) {

                    PARAMS_REDIS_UTIL.autoLog(
                        vid,
                        ()-> LOG.info(
                            "VID[{}]GPS故障时延不足[{}]毫秒",
                            vid,
                            gpsFaultIntervalMillisecond));

                    return null;
                }

                try {

                    final ImmutableMap<String, String> oldNotice = VEHICLE_CACHE.getField(
                        vid,
                        NoticeType.NO_POSITION_VEH
                    );
                    if (MapUtils.isNotEmpty(oldNotice)) {

                        final String oldStatus = oldNotice.get("status");
                        if ("1".equals(oldStatus) || "2".equals(oldStatus)) {

                            gpsFaultNotice.clear();
                            gpsFaultNotice.putAll(oldNotice);

                            LOG.info("从缓存取回GPS故障告警, 不再发送通知.");
                            return null;
                        }
                    }
                } catch (ExecutionException e) {

                    LOG.warn("获取GPS故障通知缓存异常");
                }

                gpsFaultNotice.put("status", "1");
                gpsFaultNotice.put("slazy", String.valueOf(gpsFaultIntervalMillisecond));
                gpsFaultNotice.put("noticeTime", noticeTime);

                PARAMS_REDIS_UTIL.autoLog(
                    vid,
                    ()-> LOG.info(
                        "VID[{}]GPS故障通知缓存[{}]",
                        vid,
                        gpsFaultNotice.get("msgId")));

                final ImmutableMap<String, String> notice = new ImmutableMap.Builder<String, String>()
                    .putAll(gpsFaultNotice)
                    .build();

                VEHICLE_CACHE.putField(
                    vid,
                    NoticeType.NO_POSITION_VEH,
                    notice);

                PARAMS_REDIS_UTIL.autoLog(
                    vid,
                    ()-> LOG.info(
                        "VID[{}]GPS故障通知发送[{}]",
                        vid,
                        gpsFaultNotice.get("msgId")));


                return gpsFaultNotice;
                // endregion
            } else {
                // region 定位正常逻辑

                vidGpsFaultCount.remove(vid);

                final int gpsNormalCount = vidGpsNormalCount.getOrDefault(vid, 0) + 1;
                vidGpsNormalCount.put(vid, gpsNormalCount);

                PARAMS_REDIS_UTIL.autoLog(
                    vid,
                    ()-> LOG.info(
                        "VID[{}]判定为GPS正常第[{}]次",
                        vid,
                        gpsNormalCount));

                final Map<String, String> gpsNormalNotice = vidGpsNotice.getOrDefault(vid, new TreeMap<>());
                if (MapUtils.isEmpty(gpsNormalNotice)) {
                    gpsNormalNotice.put("msgType", NoticeType.NO_POSITION_VEH);
                    gpsNormalNotice.put("msgId", UUID.randomUUID().toString());
                    gpsNormalNotice.put("vid", vid);
                    gpsNormalNotice.put("status", "0");
                    vidGpsNotice.put(vid, gpsNormalNotice);

                    PARAMS_REDIS_UTIL.autoLog(
                        vid,
                        ()-> LOG.info(
                            "VID[{}]GPS正常首帧缓存初始化",
                            vid));

                }

                // 0-初始化, 1-异常开始, 2-异常持续, 3-异常结束
                final String status = gpsNormalNotice.getOrDefault("status", "0");
                if (!"1".equals(status) && !"2".equals(status)) {

                    PARAMS_REDIS_UTIL.autoLog(
                        vid,
                        () -> LOG.info(
                            "VID[{}][{}]GPS正常不是已开始或持续中状态",
                            vid,
                            status
                        )
                    );
                    return null;
                }

                if(1 == gpsNormalCount) {

                    final String location = DataUtils.buildLocation(
                        longitudeString,
                        latitudeString
                    );

                    gpsNormalNotice.put("etime", timeString);
                    gpsNormalNotice.put("elocation", location);
                    // 兼容性暂留
                    gpsNormalNotice.put("location", location);

                    PARAMS_REDIS_UTIL.autoLog(
                        vid,
                        ()-> LOG.info(
                            "VID[{}]GPS正常首帧初始化",
                            vid));
                }

                if(gpsNormalCount < gpsNormalFrameTriggerCount) {

                    PARAMS_REDIS_UTIL.autoLog(
                        vid,
                        ()-> LOG.info(
                            "VID[{}]GPS正常连续帧数不足[{}]帧",
                            vid,
                            gpsNormalFrameTriggerCount));
                    return null;
                }

                final Long firstGpsNormalTime;
                try {
                    firstGpsNormalTime = DateUtils.parseDate(
                        gpsNormalNotice.get("etime").toString(),
                        new String[]{FormatConstant.DATE_FORMAT}).getTime();
                } catch (ParseException e) {
                    LOG.warn("解析结束时间异常", e);
                    gpsNormalNotice.put("etime", timeString);
                    return null;
                }

                if (currentTimeMillis - firstGpsNormalTime <= gpsNormalIntervalMillisecond) {

                    PARAMS_REDIS_UTIL.autoLog(
                        vid,
                        ()-> LOG.info(
                            "VID[{}]GPS正常时延不足[{}]毫秒",
                            vid,
                            gpsNormalIntervalMillisecond));
                    return null;
                }

                try {

                    final ImmutableMap<String, String> oldNotice = VEHICLE_CACHE.getField(
                        vid,
                        NoticeType.NO_POSITION_VEH
                    );
                    if (MapUtils.isNotEmpty(oldNotice)) {

                        final String oldStatus = oldNotice.get("status");
                        if ("0".equals(oldStatus) || "3".equals(oldStatus)) {

                            gpsNormalNotice.clear();
                            gpsNormalNotice.putAll(oldNotice);

                            LOG.info("从缓存取回GPS正常告警, 不再发送通知.");
                            vidGpsNotice.remove(vid);
                            return null;
                        }
                    }
                } catch (ExecutionException e) {

                    LOG.warn("获取GPS正常通知缓存异常");
                }

                gpsNormalNotice.put("status", "3");
                gpsNormalNotice.put("elazy", String.valueOf(gpsNormalIntervalMillisecond));
                gpsNormalNotice.put("noticeTime", noticeTime);

                vidGpsNotice.remove(vid);

                VEHICLE_CACHE.delField(vid, NoticeType.NO_POSITION_VEH);

                PARAMS_REDIS_UTIL.autoLog(
                    vid,
                    ()-> LOG.info(
                        "VID[{}]GPS正常通知发送",
                        vid,
                        gpsNormalNotice.get("msgId")));

                return gpsNormalNotice;
                // endregion
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @NotNull
    private String getUsefulLocationFromCache(
        @NotNull String vid)
        throws ExecutionException {

        //final ImmutableMap<String, String> orientationCache = VEHICLE_CACHE.getField(vid, VehicleCache.ORIENTATION_FIELD);
        final ImmutableMap<String, String> longitudeCache = VEHICLE_CACHE.getField(vid, VehicleCache.LONGITUDE_FIELD);
        final ImmutableMap<String, String> latitudeCache = VEHICLE_CACHE.getField(vid, VehicleCache.LATITUDE_FIELD);

        //final String orientation = orientationCache.get(VehicleCache.VALUE_DATA_KEY);
        final String longitude = longitudeCache.get(VehicleCache.VALUE_DATA_KEY);
        final String latitude = latitudeCache.get(VehicleCache.VALUE_DATA_KEY);

        return DataUtils.buildLocation(longitude, latitude);
    }

    @NotNull
    private boolean isGpsFault(
        @NotNull String vid,
        @Nullable String orientationString,
        @Nullable String longitudeString,
        @Nullable String latitudeString) {

        if (!NumberUtils.isDigits(orientationString)) {
            PARAMS_REDIS_UTIL.autoLog(
                vid,
                ()-> LOG.info(
                    "定位状态[{}]非法, 判定为GPS故障.",
                    orientationString));
            return true;
        }


        final int orientationValue = NumberUtils.toInt(orientationString);
        if(!DataUtils.isOrientationUseful(orientationValue)) {
            PARAMS_REDIS_UTIL.autoLog(
                vid,
                ()-> LOG.info(
                    "定位状态[{}]无效, 判定为GPS故障.",
                    orientationString));
            return true;
        }

        if (!NumberUtils.isDigits(longitudeString)
            || !NumberUtils.isDigits(latitudeString)) {
            PARAMS_REDIS_UTIL.autoLog(
                vid,
                ()-> LOG.info(
                    "经度[{}]纬度[{}]非法, 判定为GPS故障.",
                    longitudeString,
                    latitudeString));
            return true;
        }

        final int longitudeValue = NumberUtils.toInt(longitudeString);
        final int latitudeValue = NumberUtils.toInt(latitudeString);

        if(DataUtils.isOrientationLongitudeUseful(longitudeValue)
            && DataUtils.isOrientationLatitudeUseful(latitudeValue)) {

            PARAMS_REDIS_UTIL.autoLog(
                vid,
                ()-> LOG.info(
                    "经度[{}]纬度[{}]有效, 判定为GPS正常.",
                    longitudeString,
                    latitudeString));
            return false;
        }

        PARAMS_REDIS_UTIL.autoLog(
            vid,
            ()-> LOG.info(
                "经度[{}]纬度[{}]无效, 判定为GPS故障.",
                longitudeString,
                latitudeString));

        return true;
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
                    String noticetime = DateFormatUtils.format(new Date(), FormatConstant.DATE_FORMAT);
                    notice = new TreeMap<>();
                    notice.put("msgType", NoticeType.ON_OFF);
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
                        String noticetime = DateFormatUtils.format(new Date(), FormatConstant.DATE_FORMAT);
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
        String msgType = dat.get(DataKey.MESSAGE_TYPE);
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
                    long logout = Long.parseLong(org.apache.commons.lang.math.NumberUtils.isNumber(logoutTime) ? logoutTime : "0");
                    long login = Long.parseLong(org.apache.commons.lang.math.NumberUtils.isNumber(loginTime) ? loginTime : "0");
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
        List<Map<String, Object>> notices = new LinkedList<>();
        String noticetime = DateFormatUtils.format(new Date(now), FormatConstant.DATE_FORMAT);
        List<String> needRemoves = new LinkedList<>();
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
        List<Map<String, Object>> notices = new LinkedList<>();
        String noticetime = DateFormatUtils.format(new Date(now), FormatConstant.DATE_FORMAT);
        List<String> needRemoves = new LinkedList<>();
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
                msg = CarLowSocJudge.vidSocNotice.get(vid);
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
                Map<String, String> notice = vidGpsNotice.get(vid);
                if (null != notice) {

                    msg = new TreeMap<>();
                    msg.putAll(notice);

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
            CarLowSocJudge.vidSocNotice.remove(vid);
            vidcanNotice.remove(vid);
            vidGpsNotice.remove(vid);
            vidSpeedGtZeroNotice.remove(vid);
            vidIgniteShutNotice.remove(vid);
            vidOnOffNotice.remove(vid);

            vidFlyEd.remove(vid);
            vidFlySt.remove(vid);
            vidShut.remove(vid);
            vidIgnite.remove(vid);
            vidSpeedGtZero.remove(vid);
            vidSpeedZero.remove(vid);
            CarLowSocJudge.vidLowSocCount.remove(vid);
            CarLowSocJudge.vidNormSoc.remove(vid);
            vidNoCanCount.remove(vid);
            vidNormalCanCount.remove(vid);
            vidGpsFaultCount.remove(vid);
            vidGpsNormalCount.remove(vid);

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
//            recorder.rebootInit(db, socRedisKeys, vidSocNotice);
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
