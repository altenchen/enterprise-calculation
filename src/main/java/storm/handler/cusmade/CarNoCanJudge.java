package storm.handler.cusmade;

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
import storm.util.ObjectUtils;
import storm.util.UUIDUtils;

import java.text.ParseException;
import java.util.*;

/**
 * @author: xzp
 * @date: 2018-06-14
 * @description: 无CAN车辆审计者
 */
public final class CarNoCanJudge {

    private static int REDIS_DB_INDEX = 6;
    private static String REDIS_TABLE_NAME = "vehCache.qy.notice.can";
    private static String STATUS_KEY = "status";

    private static Logger logger = LoggerFactory.getLogger(CarNoCanJudge.class);

    private static final DataToRedis redis;
    private static final Recorder recorder;

    /**
     * 时间格式化服务
     */
    private static final TimeFormatService TIME_FORMAT_SERVICE = TimeFormatService.getInstance();

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
    public static int faultTriggerContinueCount = 3;

    /**
     * 触发CAN故障需要的持续时长
     */
    public static long faultTriggerTimeoutMillisecond = 0;

    /**
     * 触发CAN正常需要的连续帧数
     */
    public static int normalTriggerContinueCount = 3;

    /**
     * 触发CAN正常需要的持续时长
     */
    public static long normalTriggerTimeoutMillisecond = 0;

    /**
     * 判断车辆无CAN的方案
     */
    private static ICarNoCanDecide carNoCanDecide = new CarNoCanDecideDefault();


    static {
        redis = new DataToRedis();
        recorder = new RedisRecorder(redis);
    }

    /**
     * 无CAN车辆字典
     */
    private final Map<String, CarNoCanItem> carNoCanMap;

    {
        carNoCanMap = new TreeMap<>();

        final Map<String, Map<String, Object>> restoreFromRedis = new TreeMap<>();
        recorder.rebootInit(REDIS_DB_INDEX, REDIS_TABLE_NAME, restoreFromRedis);

        for(String vid : restoreFromRedis.keySet()) {
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

            }
        }
    }
    /**
     * @param data
     * @return
     */
    @NotNull
    public Map<String, Object> processFrame(@NotNull Map<String, String> data) {
        final TreeMap<String, Object> result = new TreeMap<>();

        final String vid = data.get(DataKey.VEHICLE_ID);
        final String timeString = data.get(DataKey.TIME);
        if(!ObjectUtils.isNullOrWhiteSpace(vid)
            && !ObjectUtils.isNullOrWhiteSpace(timeString)) {
            return result;
        }
        final long time;
        try {
            time = TIME_FORMAT_SERVICE.stringTimeLong(timeString);
        } catch (ParseException e) {
            e.printStackTrace();
            return result;
        }


        // 判断是否有CAN
        final ICarNoCanDecide carNoCanDecide = getCarNoCanDecide();
        final boolean hasCan = carNoCanDecide.hasCan(data);

        // 总里程
        final String totalMileage = data.get(DataKey._2202_TOTAL_MILEAGE);
        // 经度
        final String longitude = data.get(DataKey._2502_LONGITUDE);
        // 纬度
        final String latitude = data.get(DataKey._2503_LATITUDE);
        // 经纬度坐标
        final String location = DataUtils.buildLocation(longitude, latitude);


        if(!hasCan) {
            final CarNoCanItem carNoCanItem;
            if(carNoCanMap.containsKey(vid)) {
                carNoCanItem = carNoCanMap.get(vid);
            } else {
                carNoCanItem = new CarNoCanItem(vid);
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

        if(item.continueCount > FIRST_FAULT_FRAME) {
            item.continueCount = FIRST_FAULT_FRAME;
        }

        if(item.continueCount == FIRST_FAULT_FRAME) {
            item.firstFrameEnterFault(time, totalMileage, location);
        }

        if(item.continueCount > -faultTriggerContinueCount) {
            --item.continueCount;
            return;
        }

        if(item.getAlarmStatus() == AlarmStatus.Init) {
            if(time - item.getFirstFrameTime() > faultTriggerTimeoutMillisecond) {
                item.setAlarmStatus(AlarmStatus.Start);

                Date now = new Date();
                String noticetime = TIME_FORMAT_SERVICE.toDateString(now);
                final Map<String, Object> notice = item.generateNotice();
                noticeResult.put(item.vid, notice);
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

        if(item.continueCount < FIRST_NORMAL_FRAME) {
            item.continueCount = FIRST_NORMAL_FRAME;
        }

        if(item.continueCount == FIRST_NORMAL_FRAME) {
            item.firstFrameExitFault(time, totalMileage, location);
        }

        if(item.continueCount < normalTriggerContinueCount) {
            ++item.continueCount;
            return;
        }

        if(item.getAlarmStatus() == AlarmStatus.Start
            || item.getAlarmStatus() == AlarmStatus.Continue) {
            if(time - item.getFirstFrameTime() > normalTriggerTimeoutMillisecond) {
                item.setAlarmStatus(AlarmStatus.End);

                Date now = new Date();
                String noticetime = TIME_FORMAT_SERVICE.toDateString(now);
                final Map<String, Object> notice = item.generateNotice();
                noticeResult.put(item.vid, notice);
                recorder.del(REDIS_DB_INDEX, REDIS_TABLE_NAME, item.vid);
            }
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
    private final class CarNoCanItem {

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
        public final String msgId = UUIDUtils.getUUID();

        /**
         * 正常运行时创建的项
         * @param vid 车辆ID
         */
        public CarNoCanItem(String vid){
            this.vid = vid;
            this.properties = new TreeMap<>();
            this.setAlarmStatus(AlarmStatus.Init);

            // 消息类型
            this.properties.put("msgType", AlarmMessageType.NO_CAN_VEH);
            // 消息唯一ID
            this.properties.put("msgId", msgId);
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
            this.setAlarmStatus(alarmStatus);
        }

        /**
         * 进入告警的第一帧填充状态
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
            properties.put("stime", time);
            properties.put("smileage", totalMileage);
            properties.put("slocation", location);
        }

        /**
         * 退出告警的第一帧填充状态
         * @param time
         * @param totalMileage
         * @param location
         */
        public final void firstFrameExitFault(
            final long time,
            final String totalMileage,
            final String location
        ) {
            firstFrameTime = time;
            properties.put("etime", time);
            properties.put("emileage", totalMileage);
            properties.put("elocation", location);
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
