package storm.handler.cusmade;

import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dao.DataToRedis;
import storm.handler.ctx.Recorder;
import storm.handler.ctx.RedisRecorder;
import storm.service.TimeFormatService;
import storm.system.AlarmMessageType;
import storm.system.DataKey;
import storm.util.DataUtils;
import storm.util.ParamsRedisUtil;

import java.text.ParseException;
import java.util.*;

/**
 * @author: xzp
 * @date: 2018-06-14
 * @description: 无CAN车辆审计者
 */
public final class CarNoCanJudge {

    private static final Logger logger = LoggerFactory.getLogger(CarNoCanJudge.class);
    private static final ParamsRedisUtil paramsRedisUtil = ParamsRedisUtil.getInstance();

    /**
     * 时间格式化服务
     */
    private static final TimeFormatService TIME_FORMAT_SERVICE = TimeFormatService.getInstance();

    private static final int REDIS_DB_INDEX = 6;
    private static final String REDIS_TABLE_NAME = "vehCache.qy.notice.can";
    private static final String STATUS_KEY = "status";

    /**
     * CAN故障首帧计数值
     */
    private static final byte FIRST_FAULT_FRAME = -1;

    /**
     * CAN正常首帧计数值
     */
    private static final byte FIRST_NORMAL_FRAME = 1;

    /**
     * 触发CAN故障需要的连续帧数
     */
    private static int faultTriggerContinueCount = 3;

    /**
     * 触发CAN故障需要的持续时长
     */
    private static long faultTriggerTimeoutMillisecond = 0;

    /**
     * 触发CAN正常需要的连续帧数
     */
    private static int normalTriggerContinueCount = 3;

    /**
     * 触发CAN正常需要的持续时长
     */
    private static long normalTriggerTimeoutMillisecond = 0;

    /**
     * 判断车辆无CAN的方案
     */
    private static ICarNoCanDecide carNoCanDecide = new CarNoCanDecideDefault();

    private final DataToRedis redis = new DataToRedis();
    private final Recorder recorder = new RedisRecorder(redis);

    /**
     * 无CAN车辆字典
     */
    private final Map<String, CarNoCanItem> carNoCanMap = new TreeMap<>();

    // 实例初始化代码块
    {

        final Map<String, Map<String, Object>> restoreFromRedis = new TreeMap<>();
        recorder.rebootInit(REDIS_DB_INDEX, REDIS_TABLE_NAME, restoreFromRedis);

        for(String vid : restoreFromRedis.keySet()) {
            logger.info("从Redis还原无CAN车辆信息:" + vid);

            final Map<String, Object> item = restoreFromRedis.get(vid);
            try {
                final AlarmStatus status = AlarmStatus.parseOf(
                    Integer.parseInt(
                        item.get(STATUS_KEY).toString()));

                if(AlarmStatus.Start == status || AlarmStatus.Continue == status) {
                    final CarNoCanItem carNoCanItem = new CarNoCanItem(vid, item, AlarmStatus.Start);
                    carNoCanMap.put(carNoCanItem.vid, carNoCanItem);
                }
            }
            catch (Exception ignore) {
                ignore.printStackTrace();
            }
        }
    }

    /**
     * 获取触发CAN故障需要的连续帧数
     */
    @Contract(pure = true)
    public static int getFaultTriggerContinueCount() {
        return faultTriggerContinueCount;
    }

    /**
     * 设置触发CAN故障需要的连续帧数
     */
    public static void setFaultTriggerContinueCount(int faultTriggerContinueCount) {
        CarNoCanJudge.faultTriggerContinueCount = faultTriggerContinueCount;
        logger.info("触发CAN故障需要的连续帧数被设置为:" + faultTriggerContinueCount);
    }

    /**
     * 获取触发CAN故障需要的持续时长
     */
    @Contract(pure = true)
    public static long getFaultTriggerTimeoutMillisecond() {
        return faultTriggerTimeoutMillisecond;
    }

    /**
     * 设置触发CAN故障需要的持续时长
     */
    public static void setFaultTriggerTimeoutMillisecond(long faultTriggerTimeoutMillisecond) {
        CarNoCanJudge.faultTriggerTimeoutMillisecond = faultTriggerTimeoutMillisecond;
        logger.info("触发CAN故障需要的持续时长被设置为:" + faultTriggerTimeoutMillisecond);
    }

    /**
     * 获取触发CAN正常需要的连续帧数
     */
    @Contract(pure = true)
    public static int getNormalTriggerContinueCount() {
        return normalTriggerContinueCount;
    }

    /**
     * 设置触发CAN正常需要的连续帧数
     */
    public static void setNormalTriggerContinueCount(int normalTriggerContinueCount) {
        CarNoCanJudge.normalTriggerContinueCount = normalTriggerContinueCount;
        logger.info("触发CAN正常需要的连续帧数被设置为:" + normalTriggerContinueCount);
    }

    /**
     * 获取触发CAN正常需要的持续时长
     */
    @Contract(pure = true)
    public static long getNormalTriggerTimeoutMillisecond() {
        return normalTriggerTimeoutMillisecond;
    }

    /**
     * 设置触发CAN正常需要的持续时长
     */
    public static void setNormalTriggerTimeoutMillisecond(long normalTriggerTimeoutMillisecond) {
        CarNoCanJudge.normalTriggerTimeoutMillisecond = normalTriggerTimeoutMillisecond;
        logger.info("触发CAN正常需要的持续时长被设置为:" + normalTriggerTimeoutMillisecond);
    }

    /**
     * @param data 车辆数据
     * @return 如果产生无CAN通知, 则填充通知, 否则为空集合.
     */
    @NotNull
    public Map<String, Object> processFrame(@NotNull Map<String, String> data) {
        // vid, notice
        final TreeMap<String, Object> result = new TreeMap<>();

        final String vid = data.get(DataKey.VEHICLE_ID);
        final String timeString = data.get(DataKey.TIME);
        final boolean traceVehicle = paramsRedisUtil.isTraceVehicleId(vid);

        if(StringUtils.isBlank(vid)
            || StringUtils.isBlank(timeString)) {
            if(traceVehicle) {
                logger.info("无CAN放弃判定, 时间空白.");
            }
            return result;
        }
        final long time;
        try {
            time = TIME_FORMAT_SERVICE.stringTimeLong(timeString);
        } catch (ParseException e) {
            e.printStackTrace();
            if(traceVehicle) {
                logger.info("无CAN放弃判定, 时间格式错误.");
            }
            return result;
        }

        // 总里程
        final String totalMileage = data.get(DataKey._2202_TOTAL_MILEAGE);
        // 经度
        final String longitude = data.get(DataKey._2502_LONGITUDE);
        // 纬度
        final String latitude = data.get(DataKey._2503_LATITUDE);
        // 经纬度坐标
        final String location = DataUtils.buildLocation(longitude, latitude);


        // 判断是否有CAN
        final ICarNoCanDecide carNoCanDecide = getCarNoCanDecide();
        boolean hasCan = carNoCanDecide.hasCan(data);

        if(!hasCan) {
            final CarNoCanItem carNoCanItem;
            if(carNoCanMap.containsKey(vid)) {
                carNoCanItem = carNoCanMap.get(vid);
            } else {
                carNoCanItem = new CarNoCanItem(vid);
                carNoCanMap.put(vid, carNoCanItem);
            }
            continueFaultIncrement(result, carNoCanItem, time, totalMileage, location);
        } else if(carNoCanMap.containsKey(vid)) {
            final CarNoCanItem carNoCanItem = carNoCanMap.get(vid);
            continueNormalIncrement(result, carNoCanItem, time, totalMileage, location);
        }

        return result;
    }

    /**
     * 连续CAN故障自增
     */
    private void continueFaultIncrement(
        @NotNull final Map<String, Object> noticeResult,
        @NotNull final CarNoCanItem item,
        @NotNull final long time,
        @NotNull final String totalMileage,
        @NotNull final String location
    ) {
        final boolean traceVehicle = paramsRedisUtil.isTraceVehicleId(item.vid);

        if(item.continueCount > FIRST_FAULT_FRAME) {
            item.continueCount = FIRST_FAULT_FRAME;
        }

        if(item.continueCount == FIRST_FAULT_FRAME) {
            item.firstFrameEnterFault(time, totalMileage, location);
        }

        if(traceVehicle) {
            logger.info("VID[" + item.vid + "]判定为无CAN[" + item.continueCount + "]");
        }

        if(item.continueCount > -getFaultTriggerContinueCount()) {
            --item.continueCount;
            return;
        }

        final long delayMillisecond = getFaultTriggerTimeoutMillisecond();
        if(time - item.getFirstFrameTime() > delayMillisecond) {

            item.lastFrameEnterFault(delayMillisecond);

            if(item.getAlarmStatus() == AlarmStatus.End
                || item.getAlarmStatus() == AlarmStatus.Init) {

                item.setAlarmStatus(AlarmStatus.Start);

                if(traceVehicle) {
                    logger.info("VID[" + item.vid + "]触发CAN故障[" + item.getAlarmStatus() + "]");
                }

                final Map<String, Object> notice = item.generateNotice();
                noticeResult.putAll(notice);
                recorder.save(REDIS_DB_INDEX, REDIS_TABLE_NAME, item.vid, notice);
            }
        }
    }

    /**
     * 连续CAN正常自增
     */
    private void continueNormalIncrement(
        @NotNull final Map<String, Object> noticeResult,
        @NotNull final CarNoCanItem item,
        @NotNull final long time,
        @NotNull final String totalMileage,
        @NotNull final String location
    ) {
        final boolean traceVehicle = paramsRedisUtil.isTraceVehicleId(item.vid);

        if(item.continueCount < FIRST_NORMAL_FRAME) {
            item.continueCount = FIRST_NORMAL_FRAME;
        }

        if(item.continueCount == FIRST_NORMAL_FRAME) {
            item.firstFrameLeaveFault(time, totalMileage, location);
        }

        if(traceVehicle) {
            logger.info("VID[" + item.vid + "]判定为有CAN[" + item.continueCount + "]");
        }

        if(item.continueCount < getNormalTriggerContinueCount()) {
            ++item.continueCount;
            return;
        }

        final long delayMillisecond = getNormalTriggerTimeoutMillisecond();
        if(time - item.getFirstFrameTime() > delayMillisecond) {
            item.lastFrameLeaveFault(delayMillisecond);

            if(item.getAlarmStatus() == AlarmStatus.Start
                || item.getAlarmStatus() == AlarmStatus.Continue) {

                item.setAlarmStatus(AlarmStatus.End);

                if(traceVehicle) {
                    logger.info("VID[" + item.vid + "]触发CAN正常[" + item.getAlarmStatus() + "]");
                }

                final Map<String, Object> notice = item.generateNotice();
                noticeResult.putAll(notice);
                recorder.del(REDIS_DB_INDEX, REDIS_TABLE_NAME, item.vid);
            }

            carNoCanMap.remove(item.vid);
        }
    }

    @Contract(pure = true)
    public static ICarNoCanDecide getCarNoCanDecide() {
        return CarNoCanJudge.carNoCanDecide;
    }

    public static boolean setCarNoCanDecide(@NotNull ICarNoCanDecide carNoCanDecide) {
        assert carNoCanDecide != null : "车辆无CAN判定实现不能设置为空";
        final String beforeDecide = getCarNoCanDecide().getClass().getCanonicalName();
        final String afterDecide = carNoCanDecide.getClass().getCanonicalName();
        final String log = "车辆无CAN判定实现从[" + beforeDecide + "]切换到[" + afterDecide + "].";
        System.out.println("[console]" + log);
        logger.info("[logger]" + log);
        CarNoCanJudge.carNoCanDecide = carNoCanDecide;
        return true;
    }

    /**
     * 无CAN车辆被审计项
     */
    private static final class CarNoCanItem {

        private static final Logger logger = LoggerFactory.getLogger(CarNoCanItem.class);

        /**
         * 连续CAN计数, 0无效, 正数为正常计数, 负数为故障计数
         */
        public int continueCount;

        /**
         * 连续CAN首帧时间
         */
        private long firstFrameTime;

        /**
         * 告警状态
         */
        private AlarmStatus alarmStatus;

        /**
         * 车辆ID
         */
        public final String vid;

        /**
         * 属性集合
         */
        private final Map<String, Object> properties;

        /**
         * 消息唯一ID
         */
        public final String msgId = UUID.randomUUID().toString();

        /**
         * 正常运行时创建的项
         * @param vid 车辆ID
         */
        public CarNoCanItem(String vid){
            this.vid = vid;
            this.properties = new TreeMap<>();

            // 车辆Id
            this.properties.put("vid", vid);
            // 消息类型
            this.properties.put("msgType", AlarmMessageType.NO_CAN_VEH);
            // 消息唯一ID
            this.properties.put("msgId", msgId);
            // 消息状态
            this.properties.put(STATUS_KEY, AlarmStatus.Init.value);

            this.setAlarmStatus(AlarmStatus.Init);
        }

        /**
         * Storm重启后从Redis还原项
         * @param vid 车辆ID
         * @param properties 属性集合
         * @param alarmStatus 通知状态
         */
        public CarNoCanItem(String vid, Map<String, Object> properties, AlarmStatus alarmStatus) {
            this.vid = vid;
            this.properties = properties;

            // 车辆Id
            this.properties.put("vid", vid);
            // 消息状态
            this.properties.put(STATUS_KEY, alarmStatus.value);
            this.setAlarmStatus(alarmStatus);
        }

        /**
         * 进入告警的首帧填充状态
         * @param time
         * @param totalMileage
         * @param location
         */
        public final void firstFrameEnterFault(
            final long time,
            final String totalMileage,
            final String location
        ) {
            firstFrameTime = time;
            final String stime = TIME_FORMAT_SERVICE.toDateString(new Date(time));
            properties.put("stime", stime);
            properties.put("smileage", totalMileage);
            properties.put("slocation", location);
        }

        /**
         * 进入告警的末帧填充状态
         * @param delayMillisecond
         */
        public final void lastFrameEnterFault(final long delayMillisecond) {
            properties.put("sdelay", delayMillisecond / 1000);
        }

        /**
         * 退出告警的首帧填充状态
         * @param time
         * @param totalMileage
         * @param location
         */
        public final void firstFrameLeaveFault(
            final long time,
            final String totalMileage,
            final String location
        ) {
            firstFrameTime = time;
            final String etime = TIME_FORMAT_SERVICE.toDateString(new Date(time));
            properties.put("etime", etime);
            properties.put("emileage", totalMileage);
            properties.put("elocation", location);
        }

        /**
         * 退出告警的末帧填充状态
         * @param delayMillisecond
         */
        public final void lastFrameLeaveFault(final long delayMillisecond) {
            properties.put("edelay", delayMillisecond / 1000);
        }

        /**
         * 根据当前属性生成通知消息
         * @return 通知消息
         */
        @NotNull
        public final Map<String, Object> generateNotice() {
            final Date now = new Date();
            final String noticetime = TIME_FORMAT_SERVICE.toDateString(now);

            final TreeMap<String, Object> result = new TreeMap<>(this.properties);
            result.put("noticetime", noticetime);
            return result;
        }

        /**
         * @return 连续CAN首帧时间
         */
        public long getFirstFrameTime() {
            return firstFrameTime;
        }

        /**
         * 获取告警状态
         */
        public AlarmStatus getAlarmStatus() {
            return alarmStatus;
        }

        /**
         * 设置告警状态
         */
        public void setAlarmStatus(AlarmStatus alarmStatus) {
            if(this.alarmStatus == alarmStatus) {
                return;
            }

            paramsRedisUtil.autoLog(vid, vid->{
                    logger.info("VID[{}]无CAN状态从[{}]切换到[{}]", vid, this.alarmStatus, alarmStatus);
            });

            this.alarmStatus = alarmStatus;
            properties.put(STATUS_KEY, alarmStatus.value);
        }
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
        End(3),

        ;

        public final int value;

        AlarmStatus(int value) {
            this.value = value;
        }

        public static AlarmStatus parseOf(int value) {
            if(Init.value == value) {
                return Init;
            }
            if(Start.value == value) {
                return Start;
            }
            if(Continue.value == value) {
                return Continue;
            }
            if(End.value == value) {
                return End;
            }

            throw new UnknownFormatConversionException("无法识别的选项");
        }
    }
}
